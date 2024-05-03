package com.htsc.app.func;

import com.alibaba.fastjson.JSONObject;
import com.htsc.common.GmallConfig;
import com.htsc.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    //声明Phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    // value:{"database":"gmall-210426-flink","tableName":"base_trademark","after":""....}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;
        try {
            //获取SQL语句   upsert into db.tn(id,name,sex) values(...,...,...)
            String upsertSQL = genUpsertSQL(value.getString("sinkTable"),
                    value.getJSONObject("after"));

            //打印SQL语句
            System.out.println(upsertSQL);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSQL);

            //如果数据为更新数据,则先删除Redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.deleteDimInfo(value.getString("sinkTable").toUpperCase(),
                        value.getJSONObject("after").getString("id"));
            }

            //执行写入数据操作
            preparedStatement.execute();

//            new Timer().schedule(new TimerTask() {
//                @Override
//                public void run() {
//                    connection.commit();
//                }
//            }, 5000, 5000);

            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }

    // upsert into db.tn(id,name,sex) values('123','456','789')
    private String genUpsertSQL(String tableName, JSONObject after) {

        Set<String> columns = after.keySet();
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                StringUtils.join(columns, ",") + ") values('" +
                StringUtils.join(values, "','") + "')";
    }
}
