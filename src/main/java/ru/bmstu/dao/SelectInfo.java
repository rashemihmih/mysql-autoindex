package ru.bmstu.dao;

public class SelectInfo implements Entity {
    private String table;
    private String key;
    private int rows;

    public SelectInfo(String table, String key, int rows) {
        this.table = table;
        this.key = key;
        this.rows = rows;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public int getRows() {
        return rows;
    }
}
