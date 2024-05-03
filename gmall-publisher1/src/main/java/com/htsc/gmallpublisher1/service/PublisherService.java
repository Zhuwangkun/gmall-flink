package com.htsc.gmallpublisher1.service;

import java.math.BigDecimal;
import java.util.Map;

public interface PublisherService {

    BigDecimal getGmv(int date);

    Map getGmvByTm(int date, int limit);

}
