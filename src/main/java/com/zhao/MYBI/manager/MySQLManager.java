package com.zhao.MYBI.manager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * MySQL配置文件
 */

public class MySQLManager {

    private static final String URL = "jdbc:mysql://localhost:3306/mybi";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }
}
