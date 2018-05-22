package ru.bmstu.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableDaoImpl implements TableDao {

    @Override
    public List<Table> list() throws SQLException {
        List<Table> tables = new ArrayList<>();
        String query = "SHOW TABLES;";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                tables.add(new Table(resultSet.getString(1)));
            }
        }
        for (Table table : tables) {
            query = String.format("SHOW COLUMNS FROM %s;", table.getName());
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    table.getColumns().add(resultSet.getString("Field"));
                }
            }
            table.getIndexes().addAll(new IndexDaoImpl(table.getName()).list());
        }
        return tables;
    }
}
