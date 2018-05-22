package ru.bmstu.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface Dao<T extends Entity> {
    Connection connection = DBService.getInstance().getConnection();

    List<T> list() throws SQLException;
}
