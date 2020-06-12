package com.tbc.batchtask.service.impl;

import com.tbc.batchtask.function.CustomModulePvDailyFunction;
import com.tbc.batchtask.function.CustomPvDailyFunction;
import com.tbc.batchtask.service.HiveToHbase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

@Service
public class HiveToHbaseImpl implements HiveToHbase {
    @Autowired
    private SparkSession spark;
    String sqlModule = "select * from (select dt as date,param1 as module,count(1) as pv from sdgh_dwd.dwd_ps_event_module_enter_log " +
            "where dt='%s' and param1 is not null group by param1,dt) as m order by pv desc limit 10";
    String sqlPvDaily = "select dt as date,count(1) as pv from  sdgh_dwd.dwd_ps_event_module_enter_log where dt='%s' group by dt";

    @Override
    public void run(String... args) throws Exception {
        spark.sql("set spark.sql.orc.compression.codec = snappy");
        Dataset<Row> modulePvResults = spark.sql(getFormatSql(sqlModule));
        modulePvResults.foreachPartition(new CustomModulePvDailyFunction("eventclickpv"));
        Dataset<Row> pvResults = spark.sql(getFormatSql(sqlPvDaily));
        pvResults.foreachPartition(new CustomPvDailyFunction("pvdaily"));
    }


    //获取昨天日期，并将日期格式化到sql中
    public String getFormatSql(String sql){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-35);
        Date date = cal.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = format.format(date);
        String formatSql = String.format(sql, dateStr);
        return formatSql;
    }

}
