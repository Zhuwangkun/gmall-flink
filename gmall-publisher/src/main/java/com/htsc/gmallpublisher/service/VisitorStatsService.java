package com.htsc.gmallpublisher.service;

import com.htsc.gmallpublisher.bean.VisitorStats;

import java.util.List;

/**
 * Desc: 访客统计业务层接口
 */
public interface VisitorStatsService {

    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHr(int date);

}
