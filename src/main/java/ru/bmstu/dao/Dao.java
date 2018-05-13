package ru.bmstu.dao;

import java.util.List;

public interface Dao<T extends Entity> {
    void create(T entity);

    void delete(T entity);

    List<T> list();
}
