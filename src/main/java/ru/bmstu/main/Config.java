package ru.bmstu.main;

import java.util.Properties;

public class Config {
    private static final Config instance = new Config();
    private String url;
    private String user;
    private String password;

    private Config() {
    }

    public static Config getInstance() {
        return instance;
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public void initialize(Properties properties) {
        url = properties.getProperty("url");
        user = properties.getProperty("user");
        password = properties.getProperty("password");
    }
}
