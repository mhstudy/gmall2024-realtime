package com.atguigu.realtimeads.controller;

import com.atguigu.realtimeads.bean.TradeProvinceOrderAmount;
import com.atguigu.realtimeads.service.TradeStatsService;
import com.atguigu.realtimeads.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

/**
 * 交易域统计controller
 */
@RestController
public class TradeStatsController {

    @Autowired
    TradeStatsService tradeStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            // 说明请求的时候，没有传递日期参数，将当天的日期作为查询的日期
            date = DateFormatUtil.now();
        }

        BigDecimal gmv = tradeStatsService.getGMV(date);

        String json = "{\n" +
                " \"status\": 0,\n" +
                " \"data\": "+gmv+"\n" +
                "}";
        return json;
    }

    @RequestMapping("/province")
    public String getProvince(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> province = tradeStatsService.getProvince(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"msg\": \"\",\"data\": {\"mapData\": [");

        for (int i = 0; i < province.size(); i++) {
            TradeProvinceOrderAmount tradeProvinceOrderAmount = province.get(i);
            jsonB.append("{\"name\": \""+tradeProvinceOrderAmount.getProvinceName()+"\",\"value\": "+tradeProvinceOrderAmount.getOrderAmount()+"}");
            if (i < province.size() - 1) {
                jsonB.append(",");
            }
        }

        jsonB.append("],\"valueName\": \"订单数\"}}");
        return jsonB.toString();
    }
}
