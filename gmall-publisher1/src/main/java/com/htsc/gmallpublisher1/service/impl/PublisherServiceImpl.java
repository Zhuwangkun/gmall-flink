package com.htsc.gmallpublisher1.service.impl;

import com.htsc.gmallpublisher1.mapper.ProductStatsMapper;
import com.htsc.gmallpublisher1.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGmv(int date) {
        return productStatsMapper.selectGmv(date);
    }

    @Override
    public Map getGmvByTm(int date, int limit) {

        //1.构建集合用于存放最终结果数据
        HashMap<String, BigDecimal> result = new HashMap<>();

        //2.查询ClickHouse获取按照品牌分组的GMV值
        List<Map> mapList = productStatsMapper.selectGmvByTm(date, limit);

        //3.遍历集合,将每行数据取出放入结果集
        for (Map map : mapList) {
            result.put((String) map.get("tm_name"), (BigDecimal) map.get("order_amount"));
        }

        //4.返回结果
        return result;
    }
}
