package com.atguigu.gmall.realtime.common.util;

import redis.clients.jedis.Jedis;

public class Test_RedisUtil {
    public static void main(String[] args) {
        Jedis jedis = RedisUtil.getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
        RedisUtil.closeJedis(jedis);
    }
}
