package com.htsc.app.dws;

import com.htsc.bean.ProvinceStats;
import com.htsc.utils.ClickHouseUtil;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.使用DDL方式创建表读取Kafka数据   dwm_order_wide
        String groupId = "province_stats_210426";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("CREATE TABLE order_wide ( " +
                "  `province_id` BIGINT, " +
                "  `province_name` STRING, " +
                "  `province_area_code` STRING, " +
                "  `province_iso_code` STRING, " +
                "  `province_3166_2_code` STRING, " +
                "  `order_id` BIGINT, " +
                "  `split_total_amount` DECIMAL, " +
                "  `create_time` STRING, " +
                "  `rt` as TO_TIMESTAMP(create_time), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ") WITH ( " +
                MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) +
                ")");

        //打印测试
//        tableEnv.executeSql("select * from order_wide").print();

        //TODO 3.执行查询
        Table sqlQuery = tableEnv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    sum(split_total_amount) order_amount, " +
                "    count(distinct order_id) order_count, " +
                "    UNIX_TIMESTAMP() as ts " +
                "from order_wide " +
                "group by " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 4.将查询结果转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(sqlQuery, ProvinceStats.class);
        provinceStatsDataStream.print();

        //TODO 5.将数据写入ClickHouse
        provinceStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_210426 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");

    }

}
