package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // TODO 3.从kafka的topic_db主题中读取数据 创建动态表      --- kafka连接器
        readOdsDB(tableEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        // TODO 4.过滤出评论数据                               --- where   table='comment_info' type='insert'
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "    `data`['id'] id,\n" +
                "    `data`['user_id'] user_id,\n" +
                "    `data`['sku_id'] sku_id,\n" +
                "    `data`['appraise'] appraise,\n" +
                "    `data`['comment_txt'] comment_txt,\n" +
                "    ts,\n" +
                "    pt\n" +
                "from topic_db\n" +
                "where `table`='comment_info' and `type`='insert'");
        // commentInfo.execute().print();
        // 将表对象注册到指定环境中
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 5.从Hbase中读取字典数据 创建动态表               --- Hbase连接器
        readBaseDic(tableEnv);

        // TODO 6.将评论表和字典表关联                          --- lookup Join
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "    id,\n" +
                "    user_id,\n" +
                "    sku_id,\n" +
                "    appraise,\n" +
                "    dic.dic_name appraise_name,\n" +
                "    comment_txt,\n" +
                "    ts\n" +
                "FROM comment_info AS c\n" +
                "  JOIN base_dic FOR SYSTEM_TIME AS OF c.pt AS dic\n" +
                "    ON c.appraise = dic.dic_code;\n");
        // joinedTable.execute().print();

        // TODO 7.将关联的结果写到kafka主题中                   --- Upsert Kafka连接器
        // 7.1 创建动态表和写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO+" (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ")"+SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 7.2 写入操作
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

    }


}
