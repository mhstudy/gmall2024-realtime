package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 操作Redis的工具类
 * 旁路缓存
 *      思路：先从缓存中获取维度数据，如果获取到了（缓存命中），直接将缓存中的维度返回即可；如果在缓存中没有找到要关联的维度，发送请求到Hbase中查询维度。
 *          并将查询的结果放到缓存中缓存起来，方便下次查询使用
 *      选型：
 *          状态：             性能很好，维护性差
 *          redis：           性能不错，维护性好  ✅
 *      关于redis的一些设置
 *          key：    维度表名:主键的值
 *          type：   string
 *          expire： 1天      避免冷数据常驻内存，给内存带来压力
 *          注意：如果维度数据发生了变化，需要将缓存的数据清除掉
 */
public class RedisUtil {

    private static JedisPool jedisPool;

    static {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMinIdle(5);
        jedisPoolConfig.setMaxTotal(100);
        jedisPoolConfig.setMaxIdle(5);
        jedisPoolConfig.setBlockWhenExhausted(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(jedisPoolConfig, Constant.REDIS_HOST, Constant.REDIS_PORT, 10000);
    }

    // 获取Jedis
    public static Jedis getJedis() {
        System.out.println("~~~获取Jedis客户端~~~");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

    // 关闭Jedis
    public static void closeJedis(Jedis jedis) {
        System.out.println("~~~Jedis客户端关闭~~~");
        if (jedis != null) {
            jedis.close();
        }
    }

    // 从redis中取数据
    public static JSONObject readDim (Jedis jedis, String tableName, String id) {
        // 拼接key
        String key = tableName + ":" + id;
        // 根据key到redis中获取维度数据
        String dimJsonStr = jedis.get(key);
        if (StringUtils.isNotEmpty(dimJsonStr)) {
            JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
            return dimJsonObj;
        }
        return null;
    }

    // 向redis中放数据
    public static void writeDim (Jedis jedis, String tableName, String id, JSONObject dimJsonObj) {
        // 拼接key
        String key = tableName + ":" + id;

        jedis.setex(key, 24 * 60 * 60, dimJsonObj.toJSONString());
    }


    // 获取异步操作Redis连接对象
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        System.out.println("~~~获取异步操作Redis的连接~~~");
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/0");
        return redisClient.connect();
    }

    // 关闭异步操作Redis连接对象
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> asyncRedisConn) {
        if (asyncRedisConn != null && asyncRedisConn.isOpen()) {
            System.out.println("~~~关闭异步操作Redis的连接~~~");
            asyncRedisConn.close();
        }
    }

    // 以异步的方式从redis中取数据
    public static JSONObject readDimAsync (StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id) {
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        // 拼接key
        String key = tableName + ":" + id;
        try {
            String dimJsonStr = asyncCommands.get(key).get();
            if (StringUtils.isNotEmpty(dimJsonStr)) {
                JSONObject dimJsonObj = JSON.parseObject(dimJsonStr);
                return dimJsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    // 以异步的方式向redis中放数据
    public static void writeDimAsync (StatefulRedisConnection<String, String> asyncRedisConn, String tableName, String id, JSONObject dimJsonObj) {
        RedisAsyncCommands<String, String> asyncCommands = asyncRedisConn.async();
        // 拼接key
        String key = tableName + ":" + id;
        asyncCommands.setex(key, 24 * 60 * 60, dimJsonObj.toJSONString());
    }
}
