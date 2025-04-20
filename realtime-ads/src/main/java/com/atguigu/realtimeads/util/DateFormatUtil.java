package com.atguigu.realtimeads.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

public class DateFormatUtil {
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
