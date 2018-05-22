package ru.bmstu.main;

import ru.bmstu.dao.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Model {
    private static final Model instance = new Model();
    private Controller controller;
    private DBService dbService = DBService.getInstance();

    private Model() {
    }

    public static Model getInstance() {
        return instance;
    }

    public void setController(Controller controller) {
        this.controller = controller;
    }

    public void connectDb() {
        try {
            dbService.connect();
        } catch (SQLException e) {
            e.printStackTrace();
            controller.showDialog("Не удалось подключиться к БД", e.getMessage());
        }
    }

    public void calculateIndexes(String input) {
        String[] queries = input.split("\\s*;\\s*");
        TableDao tableDao = new TableDaoImpl();
        List<Table> tables;
        try {
            tables = tableDao.list();
        } catch (SQLException e) {
            e.printStackTrace();
            controller.showDialog("Не удалось получить список таблиц и индексов", e.getMessage());
            return;
        }
        for (String query : queries) {
            Map<Table, List<List<String>>> queryIndexes = new QueryAnalysis(query, tables).indexes();
            try {
                Explain explain = Explain.forQuery(query);
                System.out.println();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void calculateIndexesForQuery(String query) {

        System.out.println();
    }
}
