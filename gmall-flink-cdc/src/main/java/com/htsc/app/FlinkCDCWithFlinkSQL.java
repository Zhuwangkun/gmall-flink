package com.htsc.app;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCDCWithFlinkSQL {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用DDL方式创建表
        tableEnv.executeSql("CREATE TABLE base_trademark ( " +
                "     id INT, " +
                "     tm_name STRING, " +
                "     logo_url STRING " +
                "     ) WITH ( " +
                "     'connector' = 'mysql-cdc', " +
                "     'hostname' = 'hadoop102', " +
                "     'port' = '3306', " +
                "     'username' = 'root', " +
                "     'password' = '000000', " +
                "     'database-name' = 'gmall-210426-flink', " +
                "     'table-name' = 'base_trademark')");

        //3.执行查询
        Table table = tableEnv.sqlQuery("select * from base_trademark");

        //4.打印数据
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        //5.开启任务
        env.execute("FlinkCDCWithFlinkSQL");

    }

}
