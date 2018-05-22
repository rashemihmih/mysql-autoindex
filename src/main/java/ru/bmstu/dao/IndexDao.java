package ru.bmstu.dao;

import java.sql.SQLException;

public interface IndexDao extends Dao<Index> {
    void create(Index index) throws SQLException;

    void delete(Index index) throws SQLException;
}
