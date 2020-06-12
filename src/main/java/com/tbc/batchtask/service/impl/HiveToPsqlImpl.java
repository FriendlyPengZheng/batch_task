package com.tbc.batchtask.service.impl;

import com.tbc.batchtask.service.HiveToPsql;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

@Service
@Configuration
public class HiveToPsqlImpl implements HiveToPsql,Serializable{

    @Value("${ps.url}")
    private String url;
    @Value("${ps.user}")
    private String user;
    @Value("${ps.password}")
    private String password;

    @Autowired
    private SparkSession spark;
//TODO SQL语句后面不要加“;”,执行一次spark.sql(sql)只执行一条SQL语句（此处与shell不同，不能用“;”分割多条SQL语句）
    String ods2dwd = "set hive.exec.dynamic.partition.mode=nonstrict" +
            "insert overwrite table sdgh_dwd.dwd_uc_user_login_log partition (dt) " +
            "select *,split(login_time,' ')[0] as day from sdgh_ods.ods_uc_user_login_log where split(login_time,' ')[0]='%s'";

    //TODO where dt='%s'
    String DAU = "select lt login_type,count(*) as dau,dt as day from " +
            "(select dt," +
            "case when login_type regexp('MINA') then 'MINA_ALL' " +
            "when login_type regexp('SSO-WX') then 'WeChat' " +
            "when login_type regexp('H5') then 'WeChat' " +
            "when login_type regexp('OPEN') then 'APP' " +
            "when login_type='SSO' then 'PC' " +
            "else login_type end lt " +
            "from sdgh_dwd.dwd_uc_user_login_log) t1 " +
            "group by lt,dt";
//TODO where month='%s'
    String MAU = "select login_type,sum(dau) mau,month from (select login_type,substr(day,1,7) month,dau from sdgh_dws.dws_uc_dau) t group by login_type,month";
//TODO  dt='%s'
    String dwd2dws = "insert overwrite table sdgh_dws.dws_uc_dau partition(dt) select login_type,dau,day dt from global_temp.dws_dau_view";

    @Override
    public void run(String... args) throws Exception {
        Properties prop = getJdbcProp();
        spark.sql("set spark.sql.orc.compression.codec = snappy");
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
        //spark.sql(getFormatSql(ods2dwd));
        Dataset<Row> resultDAU = spark.sql(getFormatSql(DAU));
        resultDAU.createOrReplaceGlobalTempView("dws_dau_view");
        //spark.sql(getFormatSql(dwd2dws));
        spark.sql("select * from global_temp.dws_dau_view")
                .write().
                partitionBy("day")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sdgh_dws.dws_uc_dau");
        Dataset<Row> resultMAU = spark.sql(getFormatSqlMonth(MAU));
        resultMAU.show();
        resultMAU.createOrReplaceGlobalTempView("dws_mau_view");
        //TODO 使用这种形式将dataset写入hive时不需要提前创建表，不然会报格式错误（spark默认是parquet格式）
        spark.sql("select * from global_temp.dws_mau_view")
                .write()
                .partitionBy("month")
                .mode(SaveMode.Overwrite)
                .saveAsTable("sdgh_dws.dws_uc_mau");
        resultDAU.show();
        //TODO 后面每日计算时，将SaveMode改为Append
        //resultDAU.write().mode(SaveMode.Append).jdbc(url,"t_uc_user_login_count",prop);
        //resultMAU.write().mode(SaveMode.Append).jdbc(url,"t_uc_user_login_count_month",prop);
    }

    //获取昨天日期，并将日期格式化到sql中
    public String getFormatSql(String sql){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        Date date = cal.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = format.format(date);
        String formatSql = String.format(sql, dateStr);
        return formatSql;
    }

    public String getFormatSqlMonth(String sql){
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,-1);
        Date date = cal.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
        String dateStr = format.format(date);
        String formatSql = String.format(sql, dateStr);
        return formatSql;
    }

    public Properties getJdbcProp(){
        Properties prop = new Properties();
        prop.setProperty("url",url);
        prop.setProperty("user",user);
        prop.setProperty("password",password);
        return prop;
    }

}
