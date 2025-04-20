package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * sku粒度下单业务过程聚合统计
 *      维度：sku
 *      度量：原始金额、优惠券减免金额、活动减免金额、实付金额
 * 需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、redis、doris、DwdTradeOrderDetail、DwsTradeSkuOrderWindow
 * 开发流程
 *      基本环境准备
 *      检查点相关的设置
 *      从kafka的下单事实表中读取数据
 *      空消息的处理并且将流中的数据类型进行转换    jsonStr->jsonObj
 *      去重
 *          为什么会产生重复数据？
 *              我们是从下单事实表中读取数据的，下单事实表是由订单表、订单明细表、订单明细活动表、订单明细优惠券表 四张表组成
 *              订单明细是主表，和订单表进行关联的时候，使用的是内连接
 *              和订单活动以及订单明细优惠券表进行关联的时候，使用的是左外连接
 *              如果左外连接，左表数据先到，右表数据后到，查询结果会有3条数据
 *                  左表  null    标记为+I
 *                  左表  null    标记为-D
 *                  左表  右表    标记为+I
 *              这样的数据，发送到kafka主题，kafka主题会接收到3条消息
 *                  左表  null
 *                  null
 *                  左表  右表
 *              所以我们在从下单事实表中读取数据的时候，需要过滤空消息，并去重
 *          去重前：需要按照唯一键进行分组
 *          去重方案一：状态+定时器
 *              当第一条数据到来的时候，将数据放到状态中保存起来，并注册5s后执行定时器
 *              当第二条数据到来的时候，会用第二条的聚合时间和第一条的聚合时间进行比较，将时间大的数据放到状态中
 *              当定时器被触发执行的时候，将状态中的数据发送到下游
 *              优点：如果出现重复了，只会向下游发送一条数据，数据不会膨胀
 *              缺点：时效性差
 *          去重方案二：状态+抵消
 *              当第一条数据到来的时候，将数据放到状态中，并向下游传递
 *              当第二条数据到来的时候，将状态中影响到度量值的字段进行取反，传递到下游
 *              并且将第二条数据也向下游传递
 *              优点：实效性好
 *              缺点：如果出现重复了，向下游传递3条数据，数据出现了膨胀
 *          指定Watermark以及提取事件时间字段
 *          再次对流中数据类型进行转换   jsonObj->实体类对象  （相当于wordCount封装二元组的过程）
 *          按照统计的维度进行分组
 *          开窗
 *          聚合计算
 *          维度关联
 *              最基本的实现      HBaseUtil->getRow
 *              优化1：旁路缓存
 *                  思路：先从缓存中获取维度数据，如果从缓存中获取到了维度数据（缓存命中），直接将其作为结果进行返回。
 *                       如果在缓存中，没有找到要关联的维度，发送请求到HBase中进行查询，并将查询的结果放到缓存中缓存起来
 *                  选型：
 *                      状态      性能很好，维护性差
 *                      redis    性能不错，维护性好  ✅
 *                  关于Redis的设置
 *                      key：    维度表名:主键值
 *                      type：   String
 *                      expire： 1day    避免冷数据常驻内存，给内存带来压力
 *                      注意：如果维度数据发生了变化，需要清楚缓存       DimSinkFunction->invoke
 *              优化2：异步IO
 *                  为什么使用异步？
 *                      在flink程序中，想要提升某个算子的处理能力，可以提升这个算子的并行度，但是更多的并行度意味着需要更多的硬件资源，不可能无限制的提升，在资源有限的情况下，可以使用异步
 *                  异步的使用场景：
 *                      用外部系统的数据，扩展流中数据的时候
 *                  默认情况下，如果使用map算子，对流中数据进行处理，底层使用的是同步的处理方式。处理完一个元素后在处理下一个元素，性能较低
 *                  所以在做维度关联的时候，可以使用Flink提供的发送异步请求的API，进行异步处理
 *                  AsyncDataStream.[un]orderWait(
 *                      流,
 *                      如何发送异步请求，需要实现AsyncFunction接口,
 *                      超时时间,
 *                      时间单位
 *                  )
 *                  HBaseUtil添加异步读数据的方法
 *                  RedisUtil添加异步读写数据的方法
 *                  封装了一个模板类，专门发送异步请求进行维度关联
 *                      class DimAsyncFunction extends RichAsyncFunction[asyncInvoke] implement DimJsonFunction[getRowKey、getTableName、addDim] {
 *                          asyncInvoke:
 *                              // 创建异步编排对象，有返回值
 *                              CompletableFuture.supplyAsync
 *                              .thenApplyAsync     // 执行线程任务   有入参，有返回值
 *                              .thenAcceptAsync    // 执行线程任务   有入参，无返回值
 *                      }
 *              将数据写到Doris中
 */
public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeSkuOrderWindow().start(
                10029,
                4,
                Constant.DORIS_TABLE_DWS_TRADE_SKU_ORDER_WINDOW,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1.过滤空消息 并对流中的类型进行类型转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (jsonStr != null) {
                            collector.collect(JSON.parseObject(jsonStr));
                        }
                    }
                }
        );
        // jsonObjDS.print();
        // TODO 2.按照唯一键（订单明细id）进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));
        // TODO 3.去重
        // 去重方式1: 状态 + 定时器      缺点：时效性差 优点：如果出现重复，只会向下游发送一条数据
        /*
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 从状态中获取上次接收到的json对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            // 说明没有重复   将当前接收到的json数据放到状态中，并注册5s后执行的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = context.timerService().currentProcessingTime();
                            context.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            // 说明重复了    用当前数据的聚合时间和状态中的聚合时间进行比较，将时间大的放到状态中
                            // 伪代码
                            String lastTs = lastJsonObj.getString("聚合时间戳");
                            String curTs = jsonObj.getString("聚合时间戳");

                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 当定时器被触发执行的时候，将状态中的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );
         */

        // 去重方式2: 状态 + 抵消   优点：时效性好     缺点：如果出现重复，需要向下游传递3条数据（数据膨胀）
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 从状态中获取上次接收到的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();

                        if (lastJsonObj != null) {
                            // 说明重复了，将已经发送到下游的数据，影响到度量值的字段进行去反在传递到下游
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            collector.collect(lastJsonObj);
                        }

                        lastJsonObjState.update(jsonObj);
                        collector.collect(jsonObj);
                    }
                }
        );
        // distinctDS.print();

        // TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long l) {
                                        return jsonObj.getLong("ts") * 1000L;
                                    }
                                }
                        )
        );
        // TODO 5.再次对流中的数据进行类型转换    jsonObj->实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) throws Exception {
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");

                        Long ts = jsonObj.getLong("ts") * 1000;

                        TradeSkuOrderBean tradeSkuOrderBean = TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                        return tradeSkuOrderBean;
                    }
                }
        );
        // beanDS.print();

        // TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
        // TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        // TODO 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean bean, TradeSkuOrderBean t1) throws Exception {
                        bean.setOriginalAmount(bean.getOriginalAmount().add(t1.getOriginalAmount()));
                        bean.setActivityReduceAmount(bean.getActivityReduceAmount().add(t1.getActivityReduceAmount()));
                        bean.setCouponReduceAmount(bean.getCouponReduceAmount().add(t1.getCouponReduceAmount()));
                        bean.setOrderAmount(bean.getOrderAmount().add(t1.getOrderAmount()));
                        return bean;
                    }
                }
                , new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TradeSkuOrderBean orderBean = iterable.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        collector.collect(orderBean);
                    }
                }
        );
        // reduceDS.print();

        // TODO 9.关联sku维度
        // 维度关联最基本的实现方式
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHbaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        // 根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        // 根据维度的主键到Hbase维度表中获取对应的维度对象
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        // 将维度对象的相关属性补充到流中的对象上
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );
         */
        // 优化1：旁路缓存
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private Connection hbaseConn;
                    private Jedis jedis;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHbaseConnection();
                        jedis = RedisUtil.getJedis();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHbaseConnection(hbaseConn);
                        RedisUtil.closeJedis(jedis);
                    }

                    @Override
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        // 根据流中对象获取要关联的维度的主键
                        String skuId = orderBean.getSkuId();
                        // 根据维度的主键。先从redis中查询维度
                        JSONObject dimJsonObj = RedisUtil.readDim(jedis, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            // 如果在Redis中找到了对应的维度数据，直接作为查询结果返回
                            System.out.println("~~~从Redis中查询维度数据~~~");
                        } else {
                            // 如果在Redis中没有找到对应的维度数据，发送请求到HBase中查询对应的维度，
                            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                            if (dimJsonObj != null) {
                                // 并将查询出来的维度放到Redis中缓存起来
                                System.out.println("~~~从HBase中查询维度数据放到redis中~~~");
                                RedisUtil.writeDim(jedis, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~没有找到要关联的维度~~~");
                            }
                        }

                        // 将维度对象相关的维度属性补充到流中的对象上
                        if (dimJsonObj != null) {
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        return orderBean;
                    }
                }
        );
         */
        // 使用旁路缓存模版关联维度
        /*
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new DimMapFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }
                }
        );
         */
        // 优化2：异步IO
        /*
        // 将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作, 启用或者不启用重试。
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                // 如何去发送异步请求,实现分发请求的 AsyncFunction
                new RichAsyncFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
                    private AsyncConnection hbaseAsyncConn;
                    private StatefulRedisConnection<String, String> redisAsyncConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();
                        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeAsyncHBaseConnection(hbaseAsyncConn);
                        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
                    }

                    @Override
                    public void asyncInvoke(TradeSkuOrderBean orderBean, ResultFuture<TradeSkuOrderBean> resultFuture) throws Exception {
                        // 根据当前流中的对象获取要关联维度的主键
                        String skuId = orderBean.getSkuId();
                        // 根据维度的主键到Redis中获取维度数据
                        JSONObject dimJsonObj = RedisUtil.readDimAsync(redisAsyncConn, "dim_sku_info", skuId);
                        if (dimJsonObj != null) {
                            // 如果在Redis中找到了要关联的维度（缓存命中）。直接将命中的维度作为结果返回
                            System.out.println("~~~从Redis中获取维度数据~~~");
                        } else {
                            // 如果在Redis中没有找到要关联的维度，发送请求到HBase中查找
                            dimJsonObj = HBaseUtil.readDimAsync(hbaseAsyncConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId);
                            if(dimJsonObj != null){
                                System.out.println("~~~从HBase中获取维度数据~~~");
                                // 将查找到的维度数据放到redis中缓存起来，方便下次查询使用
                                RedisUtil.writeDimAsync(redisAsyncConn, "dim_sku_info", skuId, dimJsonObj);
                            } else {
                                System.out.println("~~~维度数据没有找到~~~");
                            }
                        }
                        // 将维度对象相关的维度属性补充到流中的对象
                        if (dimJsonObj != null){
                            orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                            orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                            orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                            orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                        }
                        // 获取数据库交互的结果并发送给 ResultFuture 的 回调 函数。 将关联后的数据传递到下游
                        resultFuture.complete(Collections.singleton(orderBean));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
         */
        // 异步IO + 模板
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // withSkuInfoDS.print();
        // TODO 10.关联spu维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDs = AsyncDataStream.unorderedWait(
                withSkuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSpuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO 11.关联tm维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
                withSpuInfoDs,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // TODO 12.关联category3维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
                withTmDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // TODO 13.关联category2维度
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        // TODO 14.关联category1维度
        SingleOutputStreamOperator<TradeSkuOrderBean> withC1DS = AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        // TODO 15.将关联的结果写到Doris中
        withC1DS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_TABLE_DWS_TRADE_SKU_ORDER_WINDOW));
    }
}
