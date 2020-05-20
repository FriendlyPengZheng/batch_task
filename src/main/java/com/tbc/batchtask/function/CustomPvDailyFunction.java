package com.tbc.batchtask.function;

import com.tbc.batchtask.bean.PvDailyTableInfo;
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

import java.util.Iterator;

public class CustomPvDailyFunction implements ForeachPartitionFunction<Row> {
    private String tableName;

    public CustomPvDailyFunction(String tableName) {
        this.tableName = tableName;
    }

    public CustomPvDailyFunction() {
    }

    @Override
    public void call(Iterator<Row> iterator) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table hbaseTable = connection.getTable(TableName.valueOf(tableName));
        PvDailyTableInfo pvDailyTableInfo = new PvDailyTableInfo("info", "date", "pv");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String rowKey = "";
            //获取字段的值
            String dateStr = row.getAs(pvDailyTableInfo.getDate());
            Long pv = row.getAs(pvDailyTableInfo.getPv());
            rowKey = dateStr;
            // System.out.println(rowKey);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(pvDailyTableInfo.getFamily()), Bytes.toBytes(pvDailyTableInfo.getDate()), Bytes.toBytes(dateStr));
            put.addColumn(Bytes.toBytes(pvDailyTableInfo.getFamily()), Bytes.toBytes(pvDailyTableInfo.getPv()), Bytes.toBytes(String.valueOf(pv)));
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
