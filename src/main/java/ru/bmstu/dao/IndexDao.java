package ru.bmstu.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IndexDao extends BaseDao<Index> {
    private String tableName;

    public IndexDao(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void create(Index entity) {
        String columns = entity.getColumns().stream().collect(Collectors.joining(", "));
        String query = String.format("CREATE INDEX %s ON %s(%s);", entity.getName(), tableName, columns);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(Index entity) {
        String query = String.format("DROP INDEX %s ON %s;", entity.getName(), tableName);
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Index> list() {
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
        } catch (SQLException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
        return indexes;
    }
}
