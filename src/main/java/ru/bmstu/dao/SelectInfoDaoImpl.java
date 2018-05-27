package ru.bmstu.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SelectInfoDaoImpl implements SelectInfoDao {
    private String query;

    public SelectInfoDaoImpl(String query) {
        this.query = query;
    }

    @Override
    public List<SelectInfo> list() throws SQLException {
        List<SelectInfo> selectInfoList = new ArrayList<>();
        String explainQuery = "EXPLAIN " + query;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(explainQuery)) {
            while (resultSet.next()) {
                String table = resultSet.getString("table");
                String key = resultSet.getString("key");
                int rows = resultSet.getInt("rows");
                selectInfoList.add(new SelectInfo(table, key, rows));
            }
        }
        return selectInfoList;
    }
}
