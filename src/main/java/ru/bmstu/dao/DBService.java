package ru.bmstu.dao;

import ru.bmstu.main.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBService {
    private static final DBService instance = new DBService();
    private Connection connection;

    private DBService() {
    }

    public static DBService getInstance() {
        return instance;
    }

    public Connection getConnection() {
        return connection;
    }

    public void connect() throws SQLException {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Config config = Config.getInstance();
        connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
    }

    public void disconnect() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignore) {
            }
        }
    }
}
