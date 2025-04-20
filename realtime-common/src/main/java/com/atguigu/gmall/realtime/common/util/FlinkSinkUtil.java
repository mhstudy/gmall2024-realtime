package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * 获取相关sink的工具类
 */
public class FlinkSinkUtil {
    // 获取kafkaSink
    public static KafkaSink<String> getKafkaSink(String topic) {
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 当前配置决定是否开启事务，保证写到Kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务id前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
                // 设置事务超时时间。   检查点超时时间 < 事务超时时间  <= 事务的最大超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        KafkaSink<Tuple2<JSONObject, TableProcessDwd>> kafkaSink = KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> tuple2, KafkaSinkContext kafkaSinkContext, Long aLong) {
                        JSONObject jsonObj = tuple2.f0;
                        TableProcessDwd tableProcessDwd = tuple2.f1;
                        String topic = tableProcessDwd.getSinkTable();
                        return new ProducerRecord<>(topic, jsonObj.toJSONString().getBytes());
                    }
                })
                // 当前配置决定是否开启事务，保证写到Kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务id前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
                // 设置事务超时时间。   检查点超时时间 < 事务超时时间  <= 事务的最大超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }

    // 扩展：如果流中类型部不确定，如何将数据写到kafka主题
    public static <T>KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> kafkaRecordSerializationSchema) {
        KafkaSink<T> kafkaSink = KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(kafkaRecordSerializationSchema)
                // 当前配置决定是否开启事务，保证写到Kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                // 设置事务id前缀
//                .setTransactionalIdPrefix("dwd_base_log_")
                // 设置事务超时时间。   检查点超时时间 < 事务超时时间  <= 事务的最大超时时间
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 60 * 1000 + "")
                .build();
        return kafkaSink;
    }

    // 获取DorisSink
    public static DorisSink<String> getDorisSink(String tableName) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.DORIS_FE_NODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE+ "."+ tableName)
                        .setUsername("root")
                        .setPassword("aaaaaa")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 批次条数: 默认 3
                        .setBufferSize(8*1024) // 批次大小: 默认 1M
                        .setCheckInterval(3000) // 批次输出间隔   三个对批次的限制是或的关系
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        return sink;
    }
}
