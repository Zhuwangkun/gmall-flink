package com.htsc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;


//数据流: web/app -> nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM)
//程  序: Mock   -> Nginx -> Logger -> Kafka(ZK) -> BaseLogApp -> Kafka -> UniqueVisitApp -> Kafka
public class UniqueVisitApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.读取 Kafka dwd_page_log 主题数据创建流
        String groupId = "unique_visit_app_210426";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO 3.将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(text -> JSON.parseObject(text));

        //TODO 4.按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程进行数据去重(过滤)
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> dateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("date-state", String.class);
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                valueStateDescriptor.enableTimeToLive(ttlConfig);
                dateState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {

                //1.获取上一跳页面ID
                String lastPage = value.getJSONObject("page").getString("last_page_id");

                //2.判断上一跳页面ID是否为Null
                if (lastPage == null) {

                    //3.获取状态数据
                    String date = dateState.value();

                    //4.将数据中的时间戳转换为日期字符串
                    String curDate = sdf.format(value.getLong("ts"));

                    //5.判断状态数据是否为Null同时判断状态数据与当前数据的日期是否相同
                    if (date == null || !date.equals(curDate)) {

                        //更新状态
                        dateState.update(curDate);

                        return true;
                    } else {

                        return false;
                    }

                } else {
                    return false;
                }
            }
        });

        //TODO 6.将数据写入Kafka
        filterDS.print();
        filterDS.map(jsonObject -> JSON.toJSONString(jsonObject))
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        //TODO 7.启动任务
        env.execute("UniqueVisitApp");

    }

}
