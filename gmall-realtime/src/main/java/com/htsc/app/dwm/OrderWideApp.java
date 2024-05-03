package com.htsc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.htsc.app.func.AsyncDimFunction;
import com.htsc.bean.OrderDetail;
import com.htsc.bean.OrderInfo;
import com.htsc.bean.OrderWide;
import com.htsc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

//数据流：web/app -> Nginx -> SpringBoot -> Mysql -> FlinkApp -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)/Phoenix(DIM) -> FlinkApp -> Kafka(DWM)
//程  序：Mock -> Mysql -> FlinkCDCApp -> Kafka -> BaseDbApp -> Kafka/Phoenix -> OrderWideApp(Redis) -> Kafka
public class OrderWideApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境,并行度设置与Kafka主题的分区数一致

//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink-cdc-210426/ck"));
//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        //TODO 2.读取 Kafka 订单和订单明细主题的数据创建流
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_210426";
        DataStreamSource<String> orderInfoStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailStrDS = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId));

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(line -> {

            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

            String create_time = orderInfo.getCreate_time();

            String[] dateTimeArr = create_time.split(" ");
            orderInfo.setCreate_date(dateTimeArr[0]);
            orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderInfo.setCreate_ts(sdf.parse(create_time).getTime());

            return orderInfo;
        });
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);
            String create_time = orderDetail.getCreate_time();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(create_time).getTime());
            return orderDetail;
        });

        //TODO 4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));

        //TODO 5.订单与订单明细双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(orderInfo -> orderInfo.getId())
                .intervalJoin(orderDetailWithWMDS.keyBy(orderDetail -> orderDetail.getOrder_id()))
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //打印测试
        orderWideDS.print("OrderWide>>>>>>>>>>>>");

        //TODO 6.关联维度信息
//        orderWideDS.map(new RichMapFunction<OrderWide, OrderWide>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//            }
//
//            @Override
//            public OrderWide map(OrderWide value) throws Exception {
//
//                Long user_id = value.getUser_id();
//                Long province_id = value.getProvince_id();
//
//                return null;
//            }
//        });

        //6.1 关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserInfoDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                new AsyncDimFunction<OrderWide>("DIM_USER_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        String gender = dimInfo.getString("GENDER");
                        orderWide.setUser_gender(gender);

                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        long ts = System.currentTimeMillis() - sdf.parse(birthday).getTime();

                        long age = ts / (1000L * 60 * 60 * 24 * 365);

                        orderWide.setUser_age((int) age);
                    }
                },
                60,
                TimeUnit.SECONDS);

        orderWideWithUserInfoDS.print("User>>>>>>>>>>");

        //6.2 关联省份维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserInfoDS,
                new AsyncDimFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException {
                        System.out.println("省份信息>>>>>>>>>>>" + dimInfo);
                        orderWide.setProvince_name(dimInfo.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }
                }, 60,
                TimeUnit.SECONDS
        );

        orderWideWithProvinceDS.print("Province>>>>>>>>>>>");

        //6.3 关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS, new AsyncDimFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //6.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS, new AsyncDimFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //6.5 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new AsyncDimFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //6.6 关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new AsyncDimFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        orderWideWithCategory3DS.print("Category3>>>>>>>>>>>>>>");

        //TODO 7.将数据写入Kafka
        orderWideWithCategory3DS
                .map(object -> JSON.toJSONString(object))
                .addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));

        //TODO 8.启动任务
        env.execute("OrderWideApp");

    }

}
