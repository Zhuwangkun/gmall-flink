package com.htsc.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.htsc.app.func.DimSinkFunction;
import com.htsc.app.func.MyStringDebeziumDeserializationSchema;
import com.htsc.app.func.TableProcessFunction;
import com.htsc.bean.TableProcess;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

//数据流：web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka/Phoenix(DWD/DIM)
//程  序：           Mock                -> Mysql -> FlinkCDCApp -> Kafka(zk) -> BaseDbApp -> Kafka/Phoenix(HBase，HDFS)
public class BaseDbApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.读取 Kafka ods_base_db主题创建主流
        String topic = "ods_base_db";
        String groupId = "base_db_app_210426";
        DataStreamSource<String> dataStreamSource = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.将主流每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = dataStreamSource.map(text -> JSON.parseObject(text));

        //TODO 4.过滤空值数据  删除数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        //TODO 5.使用 FlinkCDC 读取配置信息表
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-210426-realtime")
                .tableList("gmall-210426-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyStringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        //TODO 6.将配置信息表的流转换为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 7.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterDS.connect(broadcastStream);

        //TODO 8.处理连接后的流
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("hbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaDS = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor, outputTag));

        //TODO 9.将kafka流数据以及HBase流数据分别写入Kafka和Phoenix
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(outputTag);
        kafkaDS.print("Kafka>>>>>>>>>");
        hbaseDS.print("HBase>>>>>>>>>");

        //使用自定义Sink写出数据到Phoenix
        hbaseDS.addSink(new DimSinkFunction());

        //将数据写入Kafka
        kafkaDS.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            //element : {"database":"gmall-210426-flink","tableName":"base_trademark","after":""....}
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("after").getBytes());
            }
        }));

        //TODO 10.启动任务
        env.execute("BaseDbApp");

    }


}
