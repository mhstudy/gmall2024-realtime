package com.atguigu.gmall.realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * 事实表动态分流---处理关联后的数据
 */
public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {

    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;
    private Map<String, TableProcessDwd> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd>  mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 将配置信息预加载到程序中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDwd> list = JdbcUtil.queryList(mySQLConnection, "select * from gmall2024_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : list) {
            String sourceTable = tableProcessDwd.getSourceTable();
            String sourceType = tableProcessDwd.getSourceType();
            String key = sourceTable + ":" + sourceType;
            configMap.put(key, tableProcessDwd);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    // 处理主流数据
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        // 获取处理业务数据库表的表名
        String tableName = jsonObj.getString("table");
        // 获取操作类型
        String type = jsonObj.getString("type");
        // 拼接key
        String key = tableName + ":" + type;

        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        // 根据key到广播状态以及configMap中获取对应的配置信息
        TableProcessDwd tp = null;

        if ((tp = broadcastState.get(key)) != null
            || (tp = configMap.get(key)) != null){
            // 说明当前数据，是要动态分流的事实表数据，将data部分传递到下游
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            // 在向下游传递数据之前，过滤掉不需要传递的字段
            String sinkColumns = tp.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj, sinkColumns);
            // 在向下游传递前，将ts事件时间补充到data对象中
            Long ts = jsonObj.getLong("ts");
            dataJsonObj.put("ts", ts);
            collector.collect(Tuple2.of(dataJsonObj, tp));
        }

    }

    // 处理广播流配置数据
    @Override
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        // 获取对配置表进行操作的类型
        String op = tp.getOp();
        // 获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // 获取业务数据库的表名
        String sourceTable = tp.getSourceTable();
        // 获取业务数据库的表操作类型
        String sourceType = tp.getSourceType();
        // 拼接key
        String key = sourceTable + ":" + sourceType;
        if ("d".equals(op)) {
            // 从配置表中删除了一条数据，那么需要将广播状态以及configMap中对应的配置也删除掉
            broadcastState.remove(key);
            configMap.remove(key);
        } else {
            // 从配置表中读取数据或者添加、更新了数据  需要将最新的配置信息放到广播状态以及configMap中
            broadcastState.put(key, tp);
            configMap.put(key, tp);
        }
    }

    /**
     * 过滤掉不需要传递的字段
     *
     * @param dataJsonObj 原始数据
     * @param sinkColumns 保留的字段
     */
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
//        Iterator<Map.Entry<String, Object>> iterator = entrySet.iterator();
//
//        for (; iterator.hasNext(); ) {
//            Map.Entry<String, Object> entry = iterator.next();
//            if(!columnList.contains(entry.getKey())){
//                iterator.remove();
//            }
//        }

        entrySet.removeIf(entry -> columnList.contains(entry.getKey()));

    }

}
