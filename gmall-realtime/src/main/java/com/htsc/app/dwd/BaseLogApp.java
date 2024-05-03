package com.htsc.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流:web/app -> nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp   -> Kafka(DWD)
//程  序:mock    -> nginx -> Logger     -> Kafka(ZK)  -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.读取Kafka ods_base_log 主题数据创建流
        String topic = "ods_base_log";
        String groupId = "base_log_app_210426";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        OutputTag<String> dirtyOutPutTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyOutPutTag, value);
                }
            }
        });
        jsonObjDS.getSideOutput(dirtyOutPutTag).print("Dirty");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程实现新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //a.获取 is_new 标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //b.如果标记为"1",处理
                if ("1".equals(isNew)) {

                    //获取状态数据
                    String state = valueState.value();

                    if (state != null) {
                        //修改标记
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //更新状态
                        valueState.update("0");
                    }
                }

                return value;
            }
        });

        //TODO 6.使用 侧输出流 实现分流功能   页面  启动  曝光
        OutputTag<String> startOutPutTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutPutTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                //获取启动数据
                String start = value.getString("start");

                if (start != null) {
                    //将数据写入启动 侧输出流
                    ctx.output(startOutPutTag, value.toJSONString());
                } else {

                    //将数据写入页面  主流
                    out.collect(value.toJSONString());

                    //提取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {

                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历数组写出曝光数据到侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);
                            ctx.output(displayOutPutTag, display.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 7.将多个流分别写入Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startOutPutTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutPutTag);

        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>>>");
        displayDS.print("Display>>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));

        //TODO 8.启动任务
        env.execute("BaseLogApp");

    }

}
