package ru.bmstu.dao;

import java.util.ArrayList;
import java.util.List;

public class Table implements Entity {
    private String name;
    private List<String> columns = new ArrayList<>();
    private List<Index> indexes = new ArrayList<>();

    public Table(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Index> getIndexes() {
        return indexes;
    }
}
