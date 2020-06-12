package com.tbc.batchtask.Utiles;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DBUtiles {
    private static Connection conn = null;
    private static PreparedStatement stmt = null;
    private static ResultSet rs = null;

    public static Connection getConn() {
        try {
            Properties prop = new Properties();
            InputStream ins = DBUtiles.class.getClassLoader().getResourceAsStream("application.properties");
            prop.load(ins);
            DataSource dataSource = DruidDataSourceFactory.createDataSource(prop);
            conn = dataSource.getConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void closeAll(Connection conn, PreparedStatement pst, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
            if (pst != null) {
                pst.close();
            }
            if (rs != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Integer insertDAU(String loginType,String day,Long DAU) {
        Integer flag = 0;
        String sql = "insert into t_uc_user_login_count values(?,?,?)";
        try {
            conn = DBUtiles.getConn();
            stmt = conn.prepareStatement(sql);
            stmt.setString(1,loginType);
            stmt.setLong(2,DAU);
            stmt.setString(3,day);
            flag = stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeAll(conn, stmt, rs);
        }
        return flag;
    }

    public static void main(String[] args) {
        //System.out.println(getOsTypeNum());
        System.out.println(getConn());
    }
}
