package com.htsc.app.func;

import com.alibaba.fastjson.JSONObject;
import com.htsc.common.GmallConfig;
import com.htsc.utils.DimUtil;
import com.htsc.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements AsyncDimJoinFunction<T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;

    private String tableName;

    public AsyncDimFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        threadPoolExecutor.execute(new Runnable() {

            @Override
            public void run() {

                try {
                    //1.查询维度数据
                    String key = getKey(input);
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);

                    //2.补充维度信息
                    join(input, dimInfo);

                    //3.将补充完维度信息的数据放入流中
                    resultFuture.complete(Collections.singletonList(input));

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
