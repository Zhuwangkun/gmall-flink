package com.htsc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.htsc.bean.OrderWide;
import com.htsc.bean.PaymentInfo;
import com.htsc.bean.PaymentWide;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

//数据流：web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)/Phoenix(DIM) -> FlinkApp -> Kafka(DWM) -> FlinkApp -> Kafka(DWM)
//程  序：Mock -> Mysql -> FlinkCDCApp -> Kafka -> BaseDbApp -> Kafka/Phoenix -> OrderWideApp(Redis) -> Kafka -> PaymentWideApp -> Kafka
public class PaymentWideApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.消费 订单宽表及支付主题数据创建流
        String groupId = "payment_wide_group_210426";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> orderWideStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentInfoStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId));

        //TODO 3.将流转换为JavaBean对象
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSON.parseObject(line, OrderWide.class));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(line -> JSON.parseObject(line, PaymentInfo.class));

        //TODO 4.提取时间生成WaterMark
        SingleOutputStreamOperator<OrderWide> orderWideWithWmDS = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
            @Override
            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    return sdf.parse(element.getCreate_time()).getTime();
                } catch (ParseException e) {
                    return recordTimestamp;
                }
            }
        }));
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWmDS = paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
            @Override
            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    return sdf.parse(element.getCreate_time()).getTime();
                } catch (ParseException e) {
                    return recordTimestamp;
                }
            }
        }));

        //TODO 5.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = orderWideWithWmDS.keyBy(orderWide -> orderWide.getOrder_id())
                .intervalJoin(paymentInfoWithWmDS.keyBy(paymentInfo -> paymentInfo.getOrder_id()))
                .between(Time.seconds(-5), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide orderWide, PaymentInfo paymentInfo, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 6.将数据写入Kafka主题
        paymentWideDS.print();
        paymentWideDS.map(object -> JSON.toJSONString(object))
                .addSink(MyKafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        //TODO 7.启动任务
        env.execute("PaymentWideApp");
    }
}
