package com.atguigu.realtimeads.service.impl;

import com.atguigu.realtimeads.bean.TradeProvinceOrderAmount;
import com.atguigu.realtimeads.mapper.TradeStatsMapper;
import com.atguigu.realtimeads.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * 交易域统计service接口实现类
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;

    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvince(Integer date) {
        return tradeStatsMapper.selectProvince(date);
    }
}
