package com.tbc.batchtask;


import com.tbc.batchtask.service.HiveToHbase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class BatchTaskApplication implements CommandLineRunner {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(BatchTaskApplication.class);
        app.run(args);
    }


    @Autowired
    private HiveToHbase hiveToHbase;

    public void run(String... args) throws IOException {
       hiveToHbase.run();
    }

    //测试ps中的数据加载到hive中
//    @Autowired
//    private SparkSession spark;
//
//    public void run(String... args) throws IOException, AnalysisException {
//        Map<String,String> map = new HashMap<>();
//        map.put("driver","org.postgresql.Driver");
//        map.put("url","jdbc:postgresql://sdghhadoop03:5432/ps");
//        map.put("user","postgres");
//        map.put("password","SDGH@!hadoop3");
//        map.put("dbtable","t_ps_event_module_enter_log");
//        Dataset<Row> rowDataset= spark.read().options(map).load();
//        rowDataset.printSchema();
//        rowDataset.createGlobalTempView("temp");
//        Dataset<Row> sql = spark.sql("select id,app_id from temp where create_time >='2020-04-01' and create_time <'2020-04-02'");
//        sql.show();
//    }


}
