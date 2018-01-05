package com.wysengine.hdfs;

import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by chenzhifu on 2017/11/23.
 */
public class HiveUtil {
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void createTable(String tableName, String dataPath, String fields, List<String> yearMonthList) throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        try (Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000", "hdfs", "");
             Statement stmt = con.createStatement()) {

            System.out.println("drop table " + tableName);
            stmt.execute("DROP TABLE IF EXISTS " + tableName);

            String createTable = "create external table " + tableName + " (" + fields + ") partitioned by (yearmonth string) STORED AS PARQUET location'" + dataPath + "'";
            System.out.println(createTable);
            stmt.execute(createTable);

            if (CollectionUtils.isNotEmpty(yearMonthList)) {
                for (String yearMonth : yearMonthList) {
                    String alterTable = "alter table " + tableName + " add partition(yearmonth='" + yearMonth.replace(HdfsUtil.yearmonth + "=","") + "')";
                    System.out.println(alterTable);
                    stmt.execute(alterTable);
                }
            }
        }
    }
}
