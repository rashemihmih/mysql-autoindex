package ru.bmstu.main;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import ru.bmstu.dao.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class Model {
    private static final String SEPARATOR = "\n\n----------------------------------------------------\n\n";
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
        TableDao tableDao = new TableDaoImpl();
        List<Table> tables;
        try {
            tables = tableDao.list();
        } catch (SQLException e) {
            e.printStackTrace();
            controller.showDialog("Не удалось получить список таблиц и индексов", e.getMessage());
            return;
        }
        List<String> queries = Arrays.stream(input.split("\\s*;\\s*")).distinct().collect(Collectors.toList());
        StringBuilder result = new StringBuilder();
        Map<String, List<Pair<Table, Index>>> resultIndexes = new HashMap<>();
        Map<String, Explain> oldExplains = new HashMap<>();
        Map<String, Explain> newExplains = new HashMap<>();
//        Map<String, Long> oldTimes = new HashMap<>();
        for (String query : queries) {
            try {
//                oldTimes.put(query, measureQueryTime(query));
                oldExplains.put(query, Explain.forQuery(query));
            } catch (SQLException ignore) {
            }
        }
        for (String query : queries) {
            if (oldExplains.get(query) == null) {
                continue;
            }
            Map<Table, List<Index>> queryIndexes = new QueryAnalysis(query, tables).indexes();
            List<Pair<Table, Index>> queryResultIndexes = new ArrayList<>();
            for (Map.Entry<Table, List<Index>> entry : queryIndexes.entrySet()) {
                Table table = entry.getKey();
                List<Index> indexes = entry.getValue();
                IndexDao indexDao = new IndexDaoImpl(table.getName());
                for (Index index : indexes) {
                    try {
                        indexDao.create(index);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
                Explain newExplain;
                try {
                    newExplain = Explain.forQuery(query);
                } catch (SQLException e) {
                    e.printStackTrace();
                    continue;
                }
                newExplains.put(query, newExplain);
                for (Index index : indexes) {
                    if (newExplain.isKeyUsed(index.getName())) {
                        table.getIndexes().add(index);
                        queryResultIndexes.add(new ImmutablePair<>(table, index));
                    } else {
                        try {
                            indexDao.delete(index);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
                resultIndexes.put(query, queryResultIndexes);
            }
        }
        for (Map.Entry<String, List<Pair<Table, Index>>> entry : resultIndexes.entrySet()) {
            List<Pair<Table, Index>> tableIndexes = entry.getValue();
            Iterator<Pair<Table, Index>> iterator = tableIndexes.iterator();
            while (iterator.hasNext()) {
                Pair<Table, Index> next = iterator.next();
                for (Map.Entry<String, List<Pair<Table, Index>>> anotherEntry : resultIndexes.entrySet()) {
                    if (!anotherEntry.getKey().equals(entry.getKey())) {
                        if (anotherEntry.getValue().stream()
                                .anyMatch(index -> QueryAnalysis.isSubIndex(
                                        next.getRight().getColumns(),
                                        index.getRight().getColumns()))) {
                            try {
                                new IndexDaoImpl(next.getLeft().getName()).delete(next.getRight());
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                            iterator.remove();
                            break;
                        }
                    }
                }
            }
        }
        for (String query : queries) {
            result.append(SEPARATOR)
                    .append("Запрос:\n")
                    .append(query)
                    .append(";\n\n");
            Explain oldExplain = oldExplains.get(query);
            if (oldExplain == null) {
                result.append("Не удалось выполнить запрос\n");
                continue;
            }
            List<Pair<Table, Index>> tableIndexes = resultIndexes.get(query);
            if (tableIndexes.isEmpty()) {
                result.append("Индексы не добавлены\n");
            } else {
                result.append("Индексы:\n");
                for (Pair<Table, Index> tableIndex : tableIndexes) {
                    Table table = tableIndex.getLeft();
                    Index index = tableIndex.getRight();
                    result.append(String.format("CREATE INDEX %s ON %s(%s);\n", index.getName(), table.getName(),
                            index.getColumns().stream().collect(Collectors.joining(", "))));
                }
            }
//            Long oldTime = oldTimes.get(query);
//            if (oldTime != null) {
//                try {
//                    long newTime = measureQueryTime(query);
//                    result.append("\nВремя:\n")
//                            .append("До добавления индексов: ")
//                            .append(oldTime)
//                            .append("\nПосле добавления индексов: ")
//                            .append(newTime)
//                            .append("\n");
//                } catch (SQLException ignore) {
//                }
//            }
            Explain newExplain = newExplains.get(query);
            if (newExplain != null) {
                result.append("\nЧисло строк, проанализированных при выполнении запроса:\n")
                        .append("До добавления индексов: ")
                        .append(oldExplain.rows())
                        .append("\nПосле добавления индексов: ")
                        .append(newExplain.rows());
            }
        }
        controller.updateResult(result.toString());
    }

    private long measureQueryTime(String query) throws SQLException {
        try (Statement statement = dbService.getConnection().createStatement()) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                statement.executeQuery(query);
            }
            return (System.currentTimeMillis() - start) / 10;
        }
    }
}
