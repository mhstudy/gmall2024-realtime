package com.atguigu.realtimeads.service.impl;

import com.atguigu.realtimeads.bean.TrafficUvCt;
import com.atguigu.realtimeads.mapper.TrafficStatsMapper;
import com.atguigu.realtimeads.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 流量域统计service接口实现类
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    private TrafficStatsMapper trafficStatsMapper;

    @Override
    public List<TrafficUvCt> getChUvCt(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date, limit);
    }
}
