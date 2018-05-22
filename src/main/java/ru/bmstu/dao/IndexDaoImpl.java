package ru.bmstu.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IndexDaoImpl implements IndexDao {
    private String tableName;

    public IndexDaoImpl(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void create(Index entity) throws SQLException {
        String columns = entity.getColumns().stream().collect(Collectors.joining(", "));
        String query = String.format("CREATE INDEX %s ON %s(%s);", entity.getName(), tableName, columns);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        }
    }

    @Override
    public void delete(Index entity) throws SQLException {
        String query = String.format("DROP INDEX %s ON %s;", entity.getName(), tableName);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        }
    }

    @Override
    public List<Index> list() throws SQLException {
        String query = String.format("SHOW KEYS FROM %s;", tableName);
        List<Index> indexes = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            Index index = null;
            while (resultSet.next()) {
                if (resultSet.getInt("Seq_in_index") == 1) {
                    if (index != null) {
                        indexes.add(index);
                    }
                    index = new Index(resultSet.getString("Key_name"));
                }
                if (index != null) {
                    index.getColumns().add(resultSet.getString("Column_name"));
                }
            }
            if (index != null) {
                indexes.add(index);
            }
        }
        return indexes;
    }
}
