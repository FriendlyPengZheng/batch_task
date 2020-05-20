package com.tbc.batchtask.config;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SparkConfig{

    @Value("${spark.app.name}")
    private String appName;
    @Value("${spark.master}")
    private String masterUri;

    /**
     * 创建sparksession并且开启hive支持
     * @return
     */
    @Bean
    public SparkSession spark()

    {
        return SparkSession.builder().master(masterUri).appName(appName)
                //可以使用集群中的jar包
                //.config("spark.yarn.jars","hdfs://sdghbigdata/user/spark/jars/*")
                .enableHiveSupport().getOrCreate();

    }

    @Bean
    public SparkContext sc() {
      return  spark().sparkContext();
    }
}