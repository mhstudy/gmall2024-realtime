package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * 维度关联需要实现的接口
 * @param <T>
 */
public interface DimJoinFunction<T> {
    String getRowKey(T obj);

    String getTableName();

    void addDims(T obj, JSONObject dimJsonObj);
}
