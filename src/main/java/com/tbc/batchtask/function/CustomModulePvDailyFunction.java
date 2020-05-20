package com.tbc.batchtask.function;

import com.tbc.batchtask.bean.ModulePvDailyTableInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Iterator;

;

public class CustomModulePvDailyFunction implements ForeachPartitionFunction<Row>, Serializable {

    private String tableName;

    public CustomModulePvDailyFunction(String tableName) {
        this.tableName = tableName;
    }

    public CustomModulePvDailyFunction() {
    }

    @Override
    public void call(Iterator<Row> iterator) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table hbaseTable = connection.getTable(TableName.valueOf(tableName));
        ModulePvDailyTableInfo modulePvDailyTableInfo = new ModulePvDailyTableInfo("info", "date", "module", "pv");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String rowKey = "";
            //获取字段的值
            String dateStr = row.getAs(modulePvDailyTableInfo.getDate());
            String moduleStr = row.getAs(modulePvDailyTableInfo.getModule());
            Long pv = row.getAs(modulePvDailyTableInfo.getPv());
            rowKey = dateStr + "_" + moduleStr;
            // System.out.println(rowKey);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(modulePvDailyTableInfo.getFamily()), Bytes.toBytes(modulePvDailyTableInfo.getDate()), Bytes.toBytes(dateStr));
            put.addColumn(Bytes.toBytes(modulePvDailyTableInfo.getFamily()), Bytes.toBytes(modulePvDailyTableInfo.getModule()), Bytes.toBytes(moduleStr));
            put.addColumn(Bytes.toBytes(modulePvDailyTableInfo.getFamily()), Bytes.toBytes(modulePvDailyTableInfo.getPv()), Bytes.toBytes(String.valueOf(pv)));
            hbaseTable.put(put);
        }

        if (hbaseTable != null) {
            hbaseTable.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}






