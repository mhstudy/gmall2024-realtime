package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.DimJoinFunction;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * 维度关联旁路缓存优化后的模版抽取
 */
public abstract class DimMapFunction<T> extends RichMapFunction<T, T> implements DimJoinFunction<T> {
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
    public T map(T obj) throws Exception {
        // 根据流中的对象获取要关联的维度主键
        String key = getRowKey(obj);
        // 根据维度的主键到Redis中获取对应的维度数据
        JSONObject dimJsonObj = RedisUtil.readDim(jedis, getTableName(), key);
        if (dimJsonObj != null) {
            // 如果从Redis中找到了对应的维度(缓存命中)，直接作为对应的结果返回数据
            System.out.println("~~~从Redis中找到了" + getTableName() + "表的" + key + "数据~~~");
        } else {
            // 如果从Redis中没有找到对应的维度，需要发送请求到Hbase中去查找
            dimJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, getTableName(), key, JSONObject.class);
            if (dimJsonObj != null) {
                // 将从HBase中查找的数据放到Redis中缓存起来，方便下次使用
                System.out.println("~~~从Hbase中找到了" + getTableName() + "表的" + key + "数据~~~");
                RedisUtil.writeDim(jedis, getTableName(), key, dimJsonObj);
            } else {
                System.out.println("~~~没有找到了" + getTableName() + "表的" + key + "数据~~~");
            }
        }
        // 将查询出来的维度对象的相关属性补充到流中的对象上
        if (dimJsonObj != null) {
            addDims(obj,dimJsonObj);
        }
        return obj;
    }
}
