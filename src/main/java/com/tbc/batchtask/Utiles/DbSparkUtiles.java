package com.tbc.batchtask.Utiles;

import java.util.Properties;

public class DbSparkUtiles {

    public static Properties getDbSparkProperties(String username, String pwd, String driverName) {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", pwd);
        properties.setProperty("driver", driverName);
        return properties;
    }

}
