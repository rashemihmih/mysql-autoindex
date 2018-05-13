package ru.bmstu.dao;

import java.util.ArrayList;
import java.util.List;

public class Index implements Entity {
    private String name;
    private List<String> columns = new ArrayList<>();

    public Index(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumns() {
        return columns;
    }
}
