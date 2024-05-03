package com.htsc.gmallpublisher1.controller;

import com.alibaba.fastjson.JSON;
import com.htsc.gmallpublisher1.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@RequestMapping("/api/sugar")
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("/gmv")
    public String getGmv(@RequestParam(value = "date", defaultValue = "0") int date) {

        //1.取默认值为当前日期
        if (date == 0) {
            date = getToday();
        }

        //2.查询GMV数据
        BigDecimal gmv = publisherService.getGmv(date);

        //3.封装JSON字符串并返回数据
//        return "{" +
//                "  \"status\": 0," +
//                "  \"msg\": \"\"," +
//                "  \"data\": " + gmv + "" +
//                "}";
        HashMap<String, Object> result = new HashMap<>();

        result.put("status", 0);
        result.put("msg", "");
        result.put("data", gmv);

        return JSON.toJSONString(result);

    }

    @RequestMapping("/tm")
    public String getGmvByTm(@RequestParam(value = "date", defaultValue = "0") int date,
                             @RequestParam(value = "limit", defaultValue = "5") int limit) {

        //1.取默认值为当前日期
        if (date == 0) {
            date = getToday();
        }

        //2.查询ClickHouse中按照品牌分组的GMV
        Map gmvByTm = publisherService.getGmvByTm(date, limit);

        //3.取出品牌名称以及值
        Set tmNames = gmvByTm.keySet();
        Collection values = gmvByTm.values();

        //4.返回结果
//        return "{" +
//                "  \"status\": 0," +
//                "  \"msg\": \"\"," +
//                "  \"data\": {" +
//                "    \"categories\": [\"" +
//                StringUtils.join(tmNames, "\",\"") +
//                "\"]," +
//                "    \"series\": [" +
//                "      {" +
//                "        \"name\": \"商品品牌\"," +
//                "        \"data\": [" +
//                StringUtils.join(values, ",") +
//                "]" +
//                "      }" +
//                "    ]" +
//                "  }" +
//                "}";

        HashMap<String, Object> result = new HashMap<>();
        result.put("status", 0);
        result.put("msg", "");

        HashMap<String, Object> dataMap = new HashMap<>();
        dataMap.put("categories", tmNames);

        ArrayList<Map> seriesList = new ArrayList<>();
        HashMap<String, Object> seriesMap = new HashMap<>();
        seriesMap.put("name", "商品品牌");
        seriesMap.put("data", values);
        seriesList.add(seriesMap);
        dataMap.put("series", seriesList);

        result.put("data", dataMap);

        return JSON.toJSONString(result);

    }

    private int getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long ts = System.currentTimeMillis();
        return Integer.parseInt(sdf.format(ts));
    }

}
