package com.atguigu.realtimeads.service;

import com.atguigu.realtimeads.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * 交易域统计service接口
 */
public interface TradeStatsService {
    // 获取某天总交易额的
    BigDecimal getGMV(Integer date);

    // 获取某天各个省份交易额
    List<TradeProvinceOrderAmount> getProvince(Integer date);
}
