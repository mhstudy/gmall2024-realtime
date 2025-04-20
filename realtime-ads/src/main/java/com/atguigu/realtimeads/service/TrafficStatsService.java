package com.atguigu.realtimeads.service;

import com.atguigu.realtimeads.bean.TrafficUvCt;

import java.util.List;

/**
 * 流量域统计service接口
 */
public interface TrafficStatsService {
    // 获取某天各个渠道独立访客数
    List<TrafficUvCt> getChUvCt(Integer date, Integer limit);
}
