package ru.bmstu.dao;

import java.sql.Connection;

abstract class BaseDao<T extends Entity> implements Dao<T> {
    Connection connection = DBService.getInstance().getConnection();
}
