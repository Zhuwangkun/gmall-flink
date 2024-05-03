package com.htsc.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.htsc.bean.VisitorStats;
import com.htsc.utils.ClickHouseUtil;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

//数据流：web/app -> Nginx -> SpringBoot -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWM) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock    -> Nginx -> Logger.sh  -> Kafka(ZK)  -> BaseLogApp -> Kafka -> uv/uj -> Kafka -> VisitorStatsApp -> ClickHouse
public class VisitorStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        // --host hadoop102 --port 9999
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //TODO 2.读取Kafka 3个主题的数据创建流
        String groupId = "visitor_stats_app_210426";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageDS = env.addSource(MyKafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(MyKafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));

        //TODO 3.将3个流转换为统一的格式
        //3.1 转换UV数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUvDS = uvDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts"));

        });

        //3.2 转换UJ数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUjDS = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts"));

        });

        //3.3 转换Page数据
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPageDS = pageDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            long sv = 0L;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        //TODO 4.Union三个流
        DataStream<VisitorStats> unionDS = visitorStatsWithUvDS.union(visitorStatsWithUjDS,
                visitorStatsWithPageDS);

        //TODO 5.提取时间戳生成Watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(13))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
            @Override
            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 6.分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS
                .keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getAr(),
                        value.getCh(),
                        value.getVc(),
                        value.getIs_new());
            }
        });

        //TODO 7.开窗  聚合
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> resultDS = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {

                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                //获取数据
                VisitorStats visitorStats = input.iterator().next();

                //获取窗口信息
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String stt = sdf.format(window.getStart());
                String edt = sdf.format(window.getEnd());

                //将窗口信息补充进数据中
                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);

                //输出数据
                out.collect(visitorStats);
            }
        });

        //TODO 8.将数据写入ClickHouse
        resultDS.print();
        resultDS.addSink(ClickHouseUtil.getClickHouseSink("insert into visitor_stats_210426 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("VisitorStatsApp");
    }

}
