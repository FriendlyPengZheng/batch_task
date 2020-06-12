package com.tbc.batchtask.service.impl;

import com.tbc.batchtask.Utiles.DateUtils;
import com.tbc.batchtask.Utiles.DbSparkUtiles;
import com.tbc.batchtask.service.PsqlToHive;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.util.Properties;

//@Service
//@Configuration
@ComponentScan("com.tbc.batchtask")
public class PsqlToHiveImpl implements PsqlToHive {

    @Autowired
    private SparkSession spark;

    @Value("${ps.url}")
    private String url;
    @Value("${ps.user}")
    private String username;
    @Value("${ps.password}")
    private String password;
    @Value("${ps.driver}")
    private String driverName;
    @Value("${ps.tableList}")
    private String tableList;

    @Override
    public void run(String... args) throws Exception {
        String[] tableArr = tableList.split(",");
        for (String tableName : tableArr) {
            for (int n = 30; n < 31; n++) {
                String yesterday = DateUtils.getDate(-(n+1));
                String today = DateUtils.getDate(-n);
                String dt = yesterday;
                String dbtable = "(select *,\'" + dt + "\' dt from " + tableName + " where create_time >= \'" + yesterday + "\' and create_time < \'" + today + "\') as tmp";
                System.out.println(dbtable);
                Properties properties = DbSparkUtiles.getDbSparkProperties(username, password, driverName);
                Dataset<Row> jdbc = spark.read().jdbc(url, dbtable, properties);
                jdbc.show();
                spark.sql("use sdgh_ods");
                jdbc.write().format("orc")
                        .option("delimiter", "`")
                        .mode(SaveMode.Append)
                        .option("compression", "snappy")
                        .partitionBy("dt")
                        .saveAsTable("ods" + tableName.substring(1));
            }
        }
    }
}
