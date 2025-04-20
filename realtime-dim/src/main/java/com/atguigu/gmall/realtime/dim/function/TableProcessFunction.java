package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * 处理主流业务数据和广播流配置数据关联后的逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 将配置表中的配置信息预加载到程序configMap中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();

        String sql = "select * from gmall2024_config.table_process_dim";
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySQLConnection, sql, TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

        JdbcUtil.closeMySQLConnection(mySQLConnection);


//        // 注册驱动
//        Class.forName("com.mysql.cj.jdbc.Driver");
//        // 建立连接
//        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
//        // 获取数据库操作对象
//        String sql = "select * from gmall2024_config.table_process_dim";
//        PreparedStatement preparedStatement = conn.prepareStatement(sql);
//        // 执行sql语句
//        ResultSet result = preparedStatement.executeQuery();
//        ResultSetMetaData metaData = result.getMetaData();
//        // 处理结果集
//        while (result.next()) {
//            // 定义一个json对象，用于接收遍历出来的数据
//            JSONObject jsonObject = new JSONObject();
//            for (int i = 1; i <= metaData.getColumnCount() ; i++) {
//                String columnName = metaData.getColumnName(i);
//                Object columValue = result.getObject(i);
//                jsonObject.put(columnName, columValue);
//            }
//            // 将jsonObj转换为实体类对象，并放到configMap中
//            TableProcessDim tableProcessDim = jsonObject.toJavaObject(TableProcessDim.class);
//            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
//        }
//        // 释放资源
//        result.close();
//        preparedStatement.close();
//        conn.close();
    }

    // 处理主流业务数据          根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 获取处理的数据的表名
        String table = jsonObj.getString("table");
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        // 根据表名到广播状态中获取对应的配置信息。如果没有找到对应的配置，在尝试到configMap中获取
        TableProcessDim tableProcessDim = null;

        if ((tableProcessDim = broadcastState.get(table)) != null || (tableProcessDim = configMap.get(table)) != null) {
            // 如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据，
            // 将维度数据继续往下游传递(只需要传递data属性内容即可)
            JSONObject dataObj = jsonObj.getJSONObject("data");

            // 在向下游传递数据之前，过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataObj, sinkColumns);

            // 在向下游传递数据前，补充对维度数据的操作类型属性
            String type = jsonObj.getString("type");
            dataObj.put("type", type);
            collector.collect(Tuple2.of(dataObj, tableProcessDim));
        }
    }

    // 处理广播流信息  将配置数据放到广播状态中或者从广播状态中删除对应的配置    k:维度表名  v:一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        // 获取对配置表进行操作的类型
        String op = tp.getOp();
        // 获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        // 获取维度表名称
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            // 从配置表中删除了一条信息，将对应的配置信息也从广播状态中删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            // 对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
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
