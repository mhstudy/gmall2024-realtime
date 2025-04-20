# 尚硅谷大数据Flink实时数仓项目5.0教程丨数据仓库项目实战

<https://www.bilibili.com/video/BV1Xb421E7jg>


## DWS层


```
交易域sku下单各窗口汇总表
    维度：sku
    度量：原始金额、活动减免金额、优惠券减免金额和订单金额
    数据来源：dwd下单事实表
    
    分析：
        dwd下单事实表是由4张表组成，订单表、订单明细表、订单明细活动表、订单明细优惠券表
        订单明细是主表
        和订单表进行关联的时候，使用的内连接
        和订单明细活动以及订单明细优惠券关联的时候，使用的是左外连接
        如果是左外连接，左表数据先到，右表数据后到，会产生3条结果
            左表  null    标记为+I
            左表  null    标记为-D
            左表  右表     标记为+I
        将这样的结果发送到kafka主题中，kafka主题会接收到3条消息
            左表  null
            null
            左表  右表
           
        当我们从kafka主题中读取消息的时候，如果使用的FlinkSQL的方式读取，会自动的对空消息进行处理；
        如果使用的是FlinkAPI的方式读取，默认SimpleStringSchame是处理不了空消息的，需要我们手动实现反序列化。
        
        对于第一条和第三条，属于重复数据，我们在DWS层中，需要做去重
            去重方式1:状态 + 定时器
            去重方式2:状态 + 抵消
            
        关联商品sku相关的维度(spu、tm、category)
            最基本的维度关联
            优化1:旁路缓存
            优化2:异步IO
```

```
将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作, 启用或者不启用重试。
实现分发请求的 AsyncFunction
获取数据库交互的结果并发送给 ResultFuture 的 回调 函数
    
        AsyncDataStream.[un]orderWait(
            ds,
            如何发送异步请求,实现分发请求的 AsyncFunction
            超时时间,
            时间单位
        )

```    


```
交易域省份粒度下单各窗口汇总表
    维度：省份
    度量：订单数+订单金额
    数据来源：dwd下单事实表

```

## ADS层 接口+大屏展示
```

SpringBoot
    开发环境
    简化开发流程

Spring
    对象的创建以及对象之间关系的创建
SpringMVC
     接收请求以及进行响应
Mybatis
    和数据库打交道
    
三层架构
    表示层
    业务层
    持久层

```

```

某天的总交易额（GMV）
    - 组件
        数字翻牌器
    - 接口访问的地址以及参数
        http://localhost:8070/gmv?date=20240614
    - 返回的数据格式
        {
            "status": 0,
            "data": 1201005.6787782388
        }
    - 执行sql
        select sum(order_amount) order_amount from dws_trade_province_order_window partition par20240613    
        

某天各个省份订单金额
    - 组件
        中国省份色彩图
    - 接口访问的地址以及参数
        http://localhost:8070/province?date=20240614
    - 返回的数据格式
        {
            "status": 0,
            "msg": "",
            "data": {
                "mapData": [
                    {
                        "name": "内蒙古",
                        "value": 12
                    },
                    {
                        "name": "上海",
                        "value": 10
                    },
                    ...
                ],
                "valueName": "订单数"
            }
        }

    - 执行sql
        select province_name, sum(order_amount) order_amount from dws_trade_province_order_window partition par20240613   
        group by province_name 


        
        
```

