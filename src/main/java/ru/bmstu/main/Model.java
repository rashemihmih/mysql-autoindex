package ru.bmstu.main;

import ru.bmstu.dao.DBService;
import ru.bmstu.dao.Table;
import ru.bmstu.dao.TableDao;

import java.sql.SQLException;
import java.util.List;

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
        TableDao tableDao = new TableDao();
        List<Table> list = tableDao.list();
        for (String query : queries) {
            calculateIndexesForQuery(query);
        }
    }

    private void calculateIndexesForQuery(String query) {
        new QueryAnalysis(query);
        System.out.println();
    }
}
