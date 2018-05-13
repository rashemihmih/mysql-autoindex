package ru.bmstu.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableDao extends BaseDao<Table> {

    @Override
    public void create(Table entity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Table entity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Table> list() {
        List<Table> tables = new ArrayList<>();
        String query = "SHOW TABLES;";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                tables.add(new Table(resultSet.getString(1)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
        for (Table table : tables) {
            query = String.format("SHOW COLUMNS FROM %s;", table.getName());
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    table.getColumns().add(resultSet.getString("Field"));
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return Collections.emptyList();
            }
            table.getIndexes().addAll(new IndexDao(table.getName()).list());
        }
        return tables;
    }
}
