package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KeywordUDTF;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 搜索关键词聚合统计
 * 需要启动的进程
 *      zk、kafka、flume、doris、DwdBaseLog、DwsTrafficSourceKeywordPageViewWindow
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 4, Constant.DORIS_TABLE_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // TODO 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        // TODO 从页面日志事实表中读取数据 创建动态表 并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second " +
                ")" + SQLUtil.getKafkaDDL( Constant.TOPIC_DWD_TRAFFIC_PAGE, Constant.DORIS_TABLE_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
        // tableEnv.executeSql("select * from page_log").print();

        // TODO 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select " +
                "page['item'] full_word, " +
                "et " +
                "from page_log " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        // searchTable.execute().print();
        tableEnv.createTemporaryView("search_table", searchTable);

        // TODO 调用自定义函数完成分词 并和原表的其他字段进行join
        Table splittable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from search_table " +
                "join lateral table(ik_analyze(full_word)) on true ");
        tableEnv.createTemporaryView("split_table", splittable);
        // tableEnv.executeSql("select * from split_table").print();

        // TODO 分组、开窗、聚合
        Table resTable = tableEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyy-MM-dd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '10' second ) ) " +
                "group by window_start, window_end, keyword ");

        // resTable.execute().print();

        // TODO 将聚合的结果写到Doris中
        tableEnv.executeSql("create table "+Constant.DORIS_TABLE_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW+"(" +
                "  stt string, " +
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + "."+Constant.DORIS_TABLE_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW+"'," +
                "  'username' = 'root'," +
                "  'password' = 'aaaaaa', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert(Constant.DORIS_TABLE_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);

    }
}
