package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;


public class Test_HBaseUtil {
    public static void main(String[] args) throws IOException {
        Connection hbaseConnection = HBaseUtil.getHbaseConnection();
        JSONObject dimBaseTrademark = HBaseUtil.getRow(hbaseConnection, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class, true);
        System.out.println(dimBaseTrademark);
        HBaseUtil.closeHbaseConnection(hbaseConnection);
    }
}
