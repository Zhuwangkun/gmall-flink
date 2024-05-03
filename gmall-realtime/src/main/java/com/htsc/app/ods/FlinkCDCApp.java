package com.htsc.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.htsc.app.func.MyStringDebeziumDeserializationSchema;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDCApp {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 指定状态后端并开启CK
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //2.创建MySQL CDC Source并创建流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210426-flink")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.打印数据并将数据写入Kafka
        dataStreamSource.print();
        dataStreamSource.addSink(MyKafkaUtil.getKafkaProducer("ods_base_db"));

        //4.启动任务
        env.execute("FlinkCDCWithDataStream");

    }
}
