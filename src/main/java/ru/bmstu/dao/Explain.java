package ru.bmstu.dao;

import java.sql.SQLException;
import java.util.List;

public class Explain {
    private List<SelectInfo> selectInfoList;

    private Explain(List<SelectInfo> selectInfoList) {
        this.selectInfoList = selectInfoList;
    }

    public boolean isKeyUsed(String key) {
        return selectInfoList.stream().anyMatch(selectInfo -> key.equals(selectInfo.getKey()));
    }

    public int rows() {
        return selectInfoList.stream().mapToInt(SelectInfo::getRows).sum();
    }

    public static Explain forQuery(String query) throws SQLException {
        SelectInfoDao selectInfoDao = new SelectInfoDaoImpl(query);
        return new Explain(selectInfoDao.list());
    }
}
