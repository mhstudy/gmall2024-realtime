package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.CartAddUuBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 加购独立用户统计
 * 需要启动的进程
 *      zk、kafka、maxwell、doris、DwdTradeCartAdd、DwsTradeCartAddUuWindow
 */
public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwsTradeCartAddUuWindow().start(
                10026,
                4,
                Constant.DORIS_TABLE_DWS_TRADE_CART_ADD_UU_WINDOW,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 1.对流中的数据类型进行转换  jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        jsonObjDS.print();
        // TODO 2.指定watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
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
        // TODO 3.按照uid分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        // TODO 4.使用flink状态编程   判断是否为加购独立用户 这里不需要封装统计的实体类对象。直接将jsonObj传递到下游
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor<>("valueStateDescriptor", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        // 获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            collector.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );
        // TODO 5.开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartUUDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));
        // TODO 6.聚合计算
        SingleOutputStreamOperator<CartAddUuBean> resultDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject jsonObj, Long aLong) {
                        return ++aLong;
                    }

                    @Override
                    public Long getResult(Long aLong) {
                        return aLong;
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return 0L;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        Long cartUUCt = iterable.iterator().next();
                        String sst = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());

                        collector.collect(new CartAddUuBean(sst, edt, curDate, cartUUCt));
                    }
                }
        );

        // TODO 7.将聚合的结果写到Doris中
        resultDS.print();
        resultDS
                .map(new BeanToJsonStrMapFunction<CartAddUuBean>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_TABLE_DWS_TRADE_CART_ADD_UU_WINDOW));
    }
}
