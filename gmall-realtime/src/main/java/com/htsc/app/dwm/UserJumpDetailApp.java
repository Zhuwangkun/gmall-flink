package com.htsc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ODS) -> FLinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)

//程  序： Mock   -> Nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka -> UserJumpDetailApp -> Kafka
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.读取Kafka  dwd_page_log  主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_app_210426";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(text -> JSON.parseObject(text))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        //TODO 5.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //TODO 6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取匹配上的和超时事件
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag, new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("begin").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("begin").get(0).toJSONString();
            }
        });

        //TODO 8.拼接两个流并写入Kafka
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        unionDS.print();
        unionDS.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 9.启动任务
        env.execute("UserJumpDetailApp");

    }

}
