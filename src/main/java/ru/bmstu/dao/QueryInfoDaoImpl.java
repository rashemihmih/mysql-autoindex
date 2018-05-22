package ru.bmstu.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryInfoDaoImpl implements QueryInfoDao {
    private String query;

    public QueryInfoDaoImpl(String query) {
        this.query = query;
    }

    @Override
    public List<QueryInfo> list() throws SQLException {
        List<QueryInfo> queryInfoList = new ArrayList<>();
        String explainQuery = "EXPLAIN " + query;
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(explainQuery)) {
            while (resultSet.next()) {
                String table = resultSet.getString("table");
                String key = resultSet.getString("key");
                int rows = resultSet.getInt("rows");
                queryInfoList.add(new QueryInfo(table, key, rows));
            }
        }
        return queryInfoList;
    }
}
