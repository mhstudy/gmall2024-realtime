package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * 操作Hbase工具类
 */
public class HBaseUtil {
    // 获取Hbase连接
    public static Connection getHbaseConnection() throws IOException {
        CompoundConfiguration conf = new CompoundConfiguration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        // conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }

    // 关闭Hbase连接
    public static void closeHbaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    // 建表
    public static void createHbaseTable(Connection hbaseConn, String namespace, String tableName, String ... families) {
        if(families.length < 1) {
            System.out.println("至少需要一个列族");
            return;
        }

        try(Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)) {
                System.out.println("表空间" + namespace + "下的表" + tableName + "已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表空间" + namespace + "下的表" + tableName + "创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    // 删表
    public static void dropHbaseTable(Connection hbaseConn, String namespace, String tableName) {
        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            // 判断要删除的表是否存在
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的表空间" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除的表空间" + namespace + "下的表" + tableName + "");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向表中put数据
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表名
     * @param rowKey    rowKey
     * @param family    列族
     * @param jsonObj   要put的数据
     */
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try(Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for(String column : columns){
                String value = jsonObj.getString(column);
                if(StringUtils.isNoneEmpty(value)){
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中put数据"+ rowKey +"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    // 向表中删除数据
    public static void deleteRow(Connection hbaseConn, String namespace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try(Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中删除数据"+ rowKey +"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据Rowkey从Hbase表中查询一行数据
     * @param hbaseConn             hbase连接对象
     * @param namespace             表空间
     * @param tableName             表名
     * @param rowKey                rowkey
     * @param clazz                 将查询的一行数据 封装的类型
     * @param isUnderlineToCamel    是否将下划线转换为驼峰命名法
     * @return
     * @param <T>
     */
    public static <T> T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey, Class<T> clazz, boolean... isUnderlineToCamel){
        // 默认不执行下划线转驼峰
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                // 定义一个对象，用于封装查询出来的一行数据
                T obj = clazz.newInstance();
                for (Cell cell : cells) {
                    String columName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if(defaultIsUToC){
                        columName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columName);
                    }
                    BeanUtils.setProperty(obj, columName, columValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }


    // 获取异步操作HBase的连接对象
    public static AsyncConnection getHBaseAsyncConnection(){
        CompoundConfiguration conf = new CompoundConfiguration();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        try {
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 关闭异步操作HBase的连接对象
    public static void closeAsyncHBaseConnection(AsyncConnection asyncConn){
        if(asyncConn != null && !asyncConn.isClosed()){
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 以异步的方式从HBase维度表中查询维度数据
     * @param asyncConn     异步操作HBase连接对象
     * @param namespace     表空间
     * @param tableName     表名
     * @param rowKey        rowkey
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConn, String namespace, String tableName, String rowKey) {
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columName, columValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
