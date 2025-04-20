package com.atguigu.realtimeads.controller;

import com.atguigu.realtimeads.bean.TrafficUvCt;
import com.atguigu.realtimeads.service.TrafficStatsService;
import com.atguigu.realtimeads.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * 交易域统计controller
 */
@RestController
public class TrafficStatsController {
    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/ch")
    public String getChUvCt(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
        if (date == 0){
            date = DateFormatUtil.now();
        }

        List<TrafficUvCt> chUvCt = trafficStatsService.getChUvCt(date, limit);
        List chList = new ArrayList();
        List uvCtList = new ArrayList();

        for (TrafficUvCt trafficUvCt : chUvCt) {
            chList.add(trafficUvCt.getCh());
            uvCtList.add(trafficUvCt.getUvCt());
        }



        String json = "{\"status\": 0, \"data\": { \"categories\": [\""+ StringUtils.join(chList, "\", \"")+"\"], \n" +
                " \"service\":[{\"name\": \"渠道\", \"data\":["+StringUtils.join(uvCtList, ",")+"]}]}}";
        return json;
    }
}
