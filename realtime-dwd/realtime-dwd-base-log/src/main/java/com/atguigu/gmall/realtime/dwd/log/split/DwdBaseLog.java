package com.atguigu.gmall.realtime.dwd.log.split;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 日志分流
 * 需要启动的进程
 *  zk、kafka、flume、DwdBaseLog
 * KafkaSource：从kafka主题中读取数据
 *              通过手动维护偏移量，可以保证消费的精准一次
 * KafkaSink：向kafka主题中写入数据，也可以保证写入的精准一次。需要做到如下操作
 *            开启检查点
 *            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *            .setTransactionalIdPrefix("dwd_base_log_")
 *            .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
 *            在消费端，需要设置消费的隔离级别为读已提交 .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
 */
public class DwdBaseLog extends BaseApp {

    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 对流中的数据类型进行转换，并做简单的ETL
        // 定义侧输出流标签
        OutputTag<String> dirtyTag = new OutputTag<>("dirtyTag") {
        };

        // ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                            // 如果转换的时候，没有发生异常，说明是标准的json，将数据传递到下游
                            collector.collect(jsonObj);
                        } catch (Exception e) {
                            // 如果转换的时候，发生了异常，说明不是标准的json，属于脏数据，将其放到侧输出流中
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
//        jsonObjDS.print("标准的json");
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        dirtyDS.print("脏数据");
        // 将侧输出流中的脏数据写到Kafka主题中
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDS.sinkTo(kafkaSink);

        // TODO 对新老访客标记进行修复
        // 按照设备ID进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        // 使用Flink状态编程完成修复
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastVisitDate", String.class);
                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                // 获取is_new的值
                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                // 从状态中获取首次访问日期
                String lastVisitDate = lastVisitDateState.value();
                // 获取当前访问日期
                Long ts = jsonObj.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);

                if ("1".equals(isNew)) {
                    // 如果is_new的值为1
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        // 如果键控状态为null，认为本次是该访客首次访问 APP，将日志中 ts 对应的日期更新到状态中，不对 is_new 字段做修改；
                        lastVisitDateState.update(curVisitDate);
                    } else {
                        // 如果键控状态不为null，且首次访问日期不是当日，说明访问的是老访客，将 is_new 字段置为 0；
                        if (!lastVisitDate.equals(curVisitDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                        // 如果键控状态不为 null，且首次访问日期是当日，说明访问的是新访客，不做操作；

                    }

                } else {
                    // 如果 is_new 的值为 0
                    // 如果键控状态为 null，说明访问 APP 的是老访客但本次是该访客的页面日志首次进入程序。当前端新老访客状态标记丢失时，
                    // 日志进入程序被判定为新访客，Flink 程序就可以纠正被误判的访客状态标记，只要将状态中的日期设置为今天之前即可。本程序选择将状态更新为昨日；
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVisitDateState.update(yesterday);
                    }
                    // 如果键控状态不为 null，说明程序已经维护了首次访问日期，不做操作。
                }
                return jsonObj;
            }
        });
//        fixedDS.print();

        // TODO 分流  错误日志-错误侧输出流 启动日志-启动侧输出流 曝光日志-曝光侧输出流 动作日志-动作侧输出流 页面日志-主流
        // 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<>("errTag"){};
        OutputTag<String> startTag = new OutputTag<>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<>("actionTag"){};
        // 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        // ~~~错误日志~~~
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            // 将错误日志写到错误侧输出流
                            context.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            // ~~~启动日志~~~
                            // 将启动日志写到启动侧输出流
                            context.output(startTag, jsonObj.toJSONString());
                        } else {
                            // ~~~页面日志~~~
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // ~~~曝光日志~~~
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                // 遍历当前页面的所有曝光信息
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    // 定义一个新的json对象，用于封装遍历出来的曝光数据
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    // 将曝光日志写到曝光侧输出流
                                    context.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("display");
                            }

                            // ~~~动作日志~~~
                            JSONArray actionsArr = jsonObj.getJSONArray("actions");
                            if (actionsArr != null && actionsArr.size() > 0) {
                                // 遍历出每一个动作
                                for (int i = 0; i < actionsArr.size(); i++) {
                                    JSONObject actionJsonObj = actionsArr.getJSONObject(i);
                                    // 定义一个新的JSON对象，用于封装动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    // 将动作日志写入到动作侧输出流
                                    context.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

                            // 页面日志 写到主流中
                            collector.collect(jsonObj.toJSONString());

                        }
                    }
                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print("页面日志");
        errDS.print("错误日志");
        startDS.print("启动日志");
        displayDS.print("曝光日志");
        actionDS.print("动作日志");
        // TODO 将不同流的数据写到kafka的不同主题中
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

    }
}
