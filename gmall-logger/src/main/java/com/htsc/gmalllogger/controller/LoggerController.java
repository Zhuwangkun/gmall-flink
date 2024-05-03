package com.htsc.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

//@Controller
@RestController
@Slf4j
public class LoggerController {

    //    Logger logger = LoggerFactory.getLogger(LoggerController.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("/test")
//    @ResponseBody
    public String test1() {
        System.out.println("aaaaaaaaa");
        return "success.html";
    }

    @RequestMapping("/test2")
    public String test2(@RequestParam("nn") String name,
                        @RequestParam(value = "age", defaultValue = "18") int age) {
        System.out.println(name + ":" + age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr) {

        //1.打印
        System.out.println(jsonStr);

        //2.落盘
        log.info(jsonStr);

        //3.写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }

}
