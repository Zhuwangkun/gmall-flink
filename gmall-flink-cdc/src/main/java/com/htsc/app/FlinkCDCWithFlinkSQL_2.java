package com.htsc.app;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCDCWithFlinkSQL_2 {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用DDL方式创建表
        tableEnv.executeSql("create table xshg_bond_order_auction(\n" +
                "   id bigint,\n" +
                "   mddate string,\n" +
                "   mdtime string,\n" +
                "   securitytype int,\n" +
                "   securitysubtype string,\n" +
                "   securityid string,\n" +
                "   securityidsource int,\n" +
                "   symbol string,\n" +
                "   tradeindex bigint,\n" +
                "   tradebuyno bigint,\n" +
                "   tradesellno bigint,\n" +
                "   tradetype int,\n" +
                "   tradebsflag int,\n" +
                "   tradeprice double,\n" +
                "   tradeqty double,\n" +
                "   trademoney double,\n" +
                "   htscsecurityid string,\n" +
                "   receivedatetime bigint,\n" +
                "   channelno int,\n" +
                "   applseqnum bigint\n" +
                ") with (\n" +
                "   'connector' = 'mysql-cdc',\n" +
                "   'hostname' = '168.63.1.78',\n" +
                "   'port' = '3308',\n" +
                "   'username' = 'cams_trade',\n" +
                "   'password' = 'XMDG_cplj_2725',\n" +
                "   'database-name' = 'test',\n" +
                "   'table-name' = 'xshg_bond_order_auction'\n" +
                ")\n");

        //3.执行查询
        Table table = tableEnv.sqlQuery("select * from xshg_bond_order_auction");

        //4.打印数据
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        //5.开启任务
        env.execute("FlinkCDCWithFlinkSQL");

    }

}
