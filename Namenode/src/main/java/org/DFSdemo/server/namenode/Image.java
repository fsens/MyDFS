package org.DFSdemo.server.namenode;

import java.sql.*;

public class Image {
    String jdbcUrl; // 数据库连接URL
    String username; // 用户名
    String password; // 密码
    Connection connection;

    Image(){
        jdbcUrl = "jdbc:postgresql://localhost:5432/your_database";
        username = "your_username";
        password = "your_password";
        try {
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        }catch (SQLException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    void saveImage(){
        new Saver(connection).save();
    }

    void loadImage(){}

    void testSelect(){
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            // 执行查询
            String sql = "SELECT * FROM your_table"; // 根据你的实际情况修改查询语句
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    // 处理查询结果
                    int id = resultSet.getInt("id"); // 根据表结构修改列名
                    String name = resultSet.getString("name"); // 根据表结构修改列名
                    System.out.println("ID: " + id + ", Name: " + name);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void testUpdate(){
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            String insertSql = "INSERT INTO your_table (id, name) VALUES (1, 'John')"; // 根据你的实际情况修改插入语句
            try (Statement statement = connection.createStatement()) {
                int rowsAffected = statement.executeUpdate(insertSql);
                System.out.println(rowsAffected + " rows inserted.");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
