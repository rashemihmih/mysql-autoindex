package ru.bmstu.dao;

import java.sql.SQLException;
import java.util.List;

public class Explain {
    private List<QueryInfo> queryInfoList;

    private Explain(List<QueryInfo> queryInfoList) {
        this.queryInfoList = queryInfoList;
    }

    public boolean isKeyUsed(String key) {
        return queryInfoList.stream().anyMatch(queryInfo -> key.equals(queryInfo.getKey()));
    }

    public int rows() {
        return queryInfoList.stream().mapToInt(QueryInfo::getRows).sum();
    }

    public static Explain forQuery(String query) throws SQLException {
        QueryInfoDao queryInfoDao = new QueryInfoDaoImpl(query);
        return new Explain(queryInfoDao.list());
    }
}
