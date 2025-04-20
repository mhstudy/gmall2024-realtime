package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.dwd.db.split.function.BaseDbTableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 处理逻辑比较简单的事实表动态分流处理
 * 需要启动的进程
 *      zk、kafka、maxwell、DwdBaseDb
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 对流中的数据类型进行转换并进行简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                collector.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是标准json");
                        }
                    }
                }
        );
        // jsonObjDS.print();

        // TODO 使用FlinkCDC读取配置表中的配置信息
        // 创建MySQLSource对象
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMysqlSource("gmall2024_config", "table_process_dwd");
        // 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        // 对流中的数据进行类型转换 jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr) throws Exception {
                        // 为了处理方便 先将jsonStr->jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 获取操作类型
                        String op = jsonObj.getString("op");
                        TableProcessDwd tp = null;

                        if("d".equals(op)){
                            // 对配置表进行了删除操作  需要从before属性中获取删除前配置信息
                            tp = jsonObj.getObject("before", TableProcessDwd.class);
                        } else {
                            // 对配置表进行了读取、插入、更新操作    需要从after属性中获取配置信息
                            tp = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);

                        return tp;
                    }
                }
        );
        // tpDS.print();

        // TODO 对配置流进行广播---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        // TODO 关联主流业务数据和广播流中的配置信息---connect
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);
        // TODO 对关联后的数据进行处理---process
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
        // TODO 将处理逻辑比较简单的事实表数据写到kafka不同主题中
        // splitDS.print();
        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());

    }
}
