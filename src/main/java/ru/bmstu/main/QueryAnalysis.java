package ru.bmstu.main;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.bmstu.dao.Index;
import ru.bmstu.dao.Table;

import java.util.*;
import java.util.stream.Collectors;

public class QueryAnalysis {
    private static final String[] CONST_EXPRESSIONS = new String[] {"=", "<>", "!=", "is", "in"};
    private List<Table> tables;
    private Map<Table, List<Index>> queryIndexes = new HashMap<>();

    public QueryAnalysis(String query, List<Table> tables) {
        this.tables = tables;
        query = query.toLowerCase()
                .replaceAll("\\(", " ( ")
                .replaceAll("\\)", " ) ")
                .replaceAll("\\s+", " ")
                .replaceAll("\\s*>\\s*", " > ")
                .replaceAll("\\s*<\\s*", " < ")
                .replaceAll("\\s*=\\s*", " = ")
                .replaceAll("\\s*>=\\s*", " >= ")
                .replaceAll("\\s*<=\\s*", " <= ")
                .replaceAll("\\s*<>\\s*", " <> ")
                .replaceAll("\\s*!=\\s*", " != ")
                .replaceAll("\\s*,\\s*", ",");
        Map<Integer, List<Integer>> depthToSelectsStarts = depthToSelectStarts(query);
        Map<Integer, List<Pair<Integer, Integer>>> depthToBounds = depthToBounds(query, depthToSelectsStarts);
        List<String> simpleQueries = simpleQueries(query, depthToBounds);
        List<Map<Table, List<String>>> simpleQueryIndexes = simpleQueries.stream()
                .map(StringUtils::trim)
                .map(this::indexesForSimpleQuery)
                .collect(Collectors.toList());
        Map<Table, List<List<String>>> tableToIndexes = new HashMap<>();
        for (Map<Table, List<String>> map : simpleQueryIndexes) {
            for (Map.Entry<Table, List<String>> entry : map.entrySet()) {
                Table table = entry.getKey();
                List<String> index = entry.getValue();
                List<List<String>> indexes = tableToIndexes.get(table);
                if (indexes == null) {
                    indexes = new ArrayList<>();
                }
                indexes.add(index);
                tableToIndexes.put(table, indexes);
            }
        }
        // убираем индексы-подиндексы
        for (Map.Entry<Table, List<List<String>>> entry : tableToIndexes.entrySet()) {
            List<List<String>> indexes = entry.getValue();
            Set<Integer> remove = new HashSet<>();
            for (int i = 0; i < indexes.size(); i++) {
                for (int j = 0; j < indexes.size(); j++) {
                    if (i != j && isSubIndex(indexes.get(i), indexes.get(j))) {
                        remove.add(i);
                        break;
                    }
                }
            }
            remove.stream()
                    .sorted(Comparator.reverseOrder())
                    .forEach(i -> indexes.remove(indexes.get(i)));
        }
        // убираем индексы-подиндексы уже существующих индексов
        for (Map.Entry<Table, List<List<String>>> entry : tableToIndexes.entrySet()) {
            Table table = entry.getKey();
            List<List<String>> indexes = entry.getValue();
            List<Index> existingIndexes = table.getIndexes();
            indexes.removeIf(index ->
                    existingIndexes.stream()
                            .anyMatch(existingIndex ->
                                    existingIndex.getColumns().equals(index)
                                            || isSubIndex(index, existingIndex.getColumns())));
        }
        for (Map.Entry<Table, List<List<String>>> entry : tableToIndexes.entrySet()) {
            Table table = entry.getKey();
            List<List<String>> indexesColumns = entry.getValue();
            List<Index> indexes = new ArrayList<>();
            for (List<String> indexColumns : indexesColumns) {
                String indexName = "index_" + table.getName() + "_" +
                        indexColumns.stream().collect(Collectors.joining("_"));
                Index index = new Index(indexName);
                index.getColumns().addAll(indexColumns);
                indexes.add(index);
            }
            queryIndexes.put(table, indexes);
        }
    }

    public Map<Table, List<Index>> indexes() {
        return queryIndexes;
    }

    public static boolean isSubIndex(List<String> thisIndex, List<String> anotherIndex) {
        if (thisIndex.size() > anotherIndex.size()) {
            return false;
        }
        for (int i = 0; i < thisIndex.size(); i++) {
            if (!thisIndex.get(i).equals(anotherIndex.get(i))) {
                return false;
            }
        }
        return true;
    }

    private Map<Integer, List<Integer>> depthToSelectStarts(String query) {
        Map<Integer, List<Integer>> selectPositionToDepth = new HashMap<>();
        StringBuilder sb = new StringBuilder(query);
        int pos = 0;
        int depth = 0;
        while (sb.length() > 0) {
            if (sb.charAt(0) == '(') {
                depth++;
                sb.deleteCharAt(0);
                pos++;
            } else if (sb.charAt(0) == (')')) {
                depth--;
                sb.deleteCharAt(0);
                pos++;
            } else if (sb.indexOf("select") == 0) {
                List<Integer> selectStarts = selectPositionToDepth.get(depth);
                if (selectStarts == null) {
                    selectStarts = new ArrayList<>();
                }
                selectStarts.add(pos);
                selectPositionToDepth.put(depth, selectStarts);
                sb.delete(0, 6);
                pos += 6;
            } else {
                sb.deleteCharAt(0);
                pos++;
            }
        }
        selectPositionToDepth.values().forEach(list -> list.sort(Integer::compareTo));
        return selectPositionToDepth;
    }

    private Map<Integer, List<Pair<Integer, Integer>>> depthToBounds(String query, Map<Integer, List<Integer>> depthToSelectStarts) {
        Map<Integer, List<Pair<Integer, Integer>>> depthToBounds = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : depthToSelectStarts.entrySet()) {
            List<Integer> selectStarts = entry.getValue();
            selectStarts.add(query.length());
            List<Pair<Integer, Integer>> bounds = new ArrayList<>();
            for (int i = 0; i < selectStarts.size() - 1; i++) {
                int start = selectStarts.get(i);
                int end = selectStarts.get(i + 1);
                int depth = 0;
                for (int j = start; j < end; j++) {
                    if (query.charAt(j) == '(') {
                        depth++;
                    } else if (query.charAt(j) == (')')) {
                        depth--;
                    }
                    if (depth < 0) {
                        end = j;
                        break;
                    }
                    if (depth == query.length() - 1) {
                        end = query.length();
                        break;
                    }
                }
                bounds.add(new ImmutablePair<>(start, end));
            }
            depthToBounds.put(entry.getKey(), bounds);
        }
        return depthToBounds;
    }

    private List<String> simpleQueries(String query, Map<Integer, List<Pair<Integer, Integer>>> depthToBounds) {
        List<String> simpleQueries = new ArrayList<>();
        int depth = depthToBounds.keySet().stream().min(Integer::compareTo).orElse(0);
        List<Pair<Integer, Integer>> bounds = depthToBounds.get(depth);
        while (bounds != null) {
            List<Pair<Integer, Integer>> deeperBounds = depthToBounds.get(depth + 1);
            if (deeperBounds == null) {
                simpleQueries.addAll(bounds.stream()
                        .map(pair -> query.substring(pair.getLeft(), pair.getRight()))
                        .collect(Collectors.toList()));
            } else {
                for (Pair<Integer, Integer> pair : bounds) {
                    Integer start = pair.getLeft();
                    Integer end = pair.getRight();
                    StringBuilder sb = new StringBuilder();
                    List<Integer> positions = new ArrayList<>();
                    positions.add(start);
                    for (Pair<Integer, Integer> deeperPair : deeperBounds) {
                        Integer deeperStart = deeperPair.getLeft();
                        Integer deeperEnd = deeperPair.getRight();
                        if (deeperStart >= start && deeperEnd <= end) {
                            positions.add(deeperStart);
                            positions.add(deeperEnd);
                        }
                    }
                    if (!positions.get(positions.size() - 1).equals(end)) {
                        positions.add(end);
                    }
                    for (int i = 0; i < positions.size() - 1; i += 2) {
                        sb.append(query, positions.get(i), positions.get(i + 1));
                    }
                    simpleQueries.add(sb.toString());
                }
            }
            bounds = deeperBounds;
            depth++;
        }
        return simpleQueries;
    }

    private Map<Table, List<String>> indexesForSimpleQuery(String query) {
        Map<Table, List<String>> indexes = new HashMap<>();
        int wherePos = pos(query, "where");
        int orderPos = pos(query, "order by");
        int groupPos = pos(query, "group by");
        int fromPos = pos(query, "from");
        if (wherePos > 0) {
            String from = query.substring(fromPos + 5, wherePos);
            Map<Table, Set<String>> tableJoins = tableJoins(from);
            int orPos = pos(query, "or");
            if (orPos > 0) {
                addIndexesForJoins(indexes, tableJoins);
            } else {
                String where;
                if (groupPos > 0) {
                    where = query.substring(wherePos + 6, groupPos);
                } else if (orderPos > 0) {
                    where = query.substring(wherePos + 6, orderPos);
                } else {
                    where = query.substring(wherePos + 6);
                }
                where = StringUtils.trim(where);
                List<Table> tables = new ArrayList<>(tableJoins.keySet());
                for (Table table : tables) {
                    indexes.put(table, new ArrayList<>());
                }
//                Arrays.stream(where.split(" "))
//                        .map(s -> tableForColumn(s, tables))
//                        .filter(Objects::nonNull)
//                        .forEach(tableCol -> indexes.get(tableCol.getLeft()).add(tableCol.getRight()));
//                if (orderPos > 0) {
//                    String order = query.substring(orderPos + 9);
//                    Arrays.stream(order.split(" "))
//                            .map(s -> tableForColumn(s, tables))
//                            .filter(Objects::nonNull)
//                            .forEach(tableCol -> {
//                                List<String> indexCols = indexes.get(tableCol.getLeft());
//                                if (!indexCols.isEmpty() && !indexCols.contains(tableCol.getRight())) {
//                                    indexCols.add(tableCol.getRight());
//                                }
//                            });
//                }
                String[] whereWords = where.split(" ");
                for (int i = 0; i < whereWords.length - 1; i++) {
                    String word = whereWords[i];
                    Pair<Table, String> tableCol = tableForColumn(word, tables);
                    if (tableCol != null) {
                        String column = tableCol.getRight();
                        if (isConstExpression(whereWords[i + 1])) {
                            Table table = tableCol.getLeft();
                            List<String> indexCols = indexes.get(table);
                            if (!indexCols.contains(column)) {
                                indexCols.add(column);
                            }
                        }
                    }
                }
                boolean containsRange = false;
                for (int i = 0; i < whereWords.length - 1 && !containsRange; i++) {
                    String word = whereWords[i];
                    Pair<Table, String> tableCol = tableForColumn(word, tables);
                    if (tableCol != null) {
                        String column = tableCol.getRight();
                        if (!isConstExpression(whereWords[i + 1])) {
                            Table table = tableCol.getLeft();
                            List<String> indexCols = indexes.get(table);
                            if (!indexCols.contains(column)) {
                                indexCols.add(column);
                                containsRange = true;
                            }
                        }
                    }
                }
                if (!containsRange) {
                    if (groupPos > 0) {
                        String group;
                        if (orderPos > 0) {
                            group = query.substring(groupPos + 9, orderPos);
                        } else {
                            int limitPos = pos(query, "limit");
                            if (limitPos > 0) {
                                group = query.substring(groupPos + 9, limitPos);
                            } else {
                                group = query.substring(groupPos + 9);
                            }
                        }
                        group = StringUtils.trim(group);
                        for (String word : group.split(",")) {
                            Pair<Table, String> tableCol = tableForColumn(word, tables);
                            if (tableCol != null) {
                                Table table = tableCol.getLeft();
                                String column = tableCol.getRight();
                                List<String> indexCols = indexes.get(table);
                                if (!indexCols.contains(column)) {
                                    indexCols.add(column);
                                }
                            }
                        }
                    } else if (orderPos > 0) {
                        List<Pair<Table, String>> orderTableCols = new ArrayList<>();
                        String order = query.substring(orderPos + 9);
                        boolean sortAsc = false;
                        boolean sortDesc = false;
                        for (String orderPart : order.split(",")) {
                            String[] colOrder = orderPart.split(" ");
                            Pair<Table, String> tableCol = tableForColumn(colOrder[0], tables);
                            if (tableCol != null) {
                                orderTableCols.add(tableCol);
                                if (colOrder.length > 1 && "desc".equals(colOrder[1])) {
                                    sortDesc = true;
                                } else {
                                    sortAsc = true;
                                }
                            }
                            if (sortAsc && sortDesc) {
                                break;
                            }
                        }
                        if (!(sortAsc && sortDesc)) {
                            for (Pair<Table, String> tableCol : orderTableCols) {
                                String column = tableCol.getRight();
                                List<String> indexCols = indexes.get(tableCol.getLeft());
                                if (!indexCols.contains(column)) {
                                    indexCols.add(column);
                                }
                            }
                        }
                    }
                }
                for (Map.Entry<Table, List<String>> entry : indexes.entrySet()) {
                    List<String> indexCols = entry.getValue();
                    if (indexCols.isEmpty()) {
                        tableJoins.get(entry.getKey()).stream().findAny().ifPresent(indexCols::add);
                    }
                }
                indexes.entrySet().removeIf(entry -> entry.getValue().isEmpty());
            }
        } else if (groupPos > 0) {
            String from = query.substring(fromPos + 5, groupPos);
            Map<Table, Set<String>> tableJoins = tableJoins(from);
//            if (tableJoins.size() > 1) {
//                addIndexesForJoins(indexes, tableJoins);
//            } else {
//                Table table = tableJoins.keySet().stream().findAny().orElse(null);
//                if (table == null) {
//                    return indexes;
//                }
//                String group = query.substring(groupPos + 9);
//                addIndexesForSubsequentColumns(indexes, group, table);
//            }
            List<Table> tables = new ArrayList<>(tableJoins.keySet());
            for (Table table : tables) {
                indexes.put(table, new ArrayList<>());
            }
            String group;
            if (orderPos > 0) {
                group = query.substring(groupPos + 9, orderPos);
            } else {
                int limitPos = pos(query, "limit");
                if (limitPos > 0) {
                    group = query.substring(groupPos + 9, limitPos);
                } else {
                    group = query.substring(groupPos + 9);
                }
            }
            group = StringUtils.trim(group);
            Table firstTable = null;
            for (String word : group.split(",")) {
                Pair<Table, String> tableCol = tableForColumn(word, tables);
                if (tableCol != null) {
                    Table table = tableCol.getLeft();
                    if (firstTable == null) {
                        firstTable = table;
                    } else if (!firstTable.equals(table)) {
                        addIndexesForJoins(indexes, tableJoins);
                        break;
                    }
                    String column = tableCol.getRight();
                    List<String> indexCols = indexes.get(table);
                    if (!indexCols.contains(column)) {
                        indexCols.add(column);
                    }
                }
            }
            for (Map.Entry<Table, List<String>> entry : indexes.entrySet()) {
                List<String> indexCols = entry.getValue();
                if (indexCols.isEmpty()) {
                    tableJoins.get(entry.getKey()).stream().findAny().ifPresent(indexCols::add);
                }
            }
            indexes.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        } else if (orderPos > 0) {
            String from = query.substring(fromPos + 4, orderPos);
            Map<Table, Set<String>> tableJoins = tableJoins(from);
//            if (tableJoins.size() > 1) {
//                addIndexesForJoins(indexes, tableJoins);
//            } else {
//                Table table = tableJoins.keySet().stream().findAny().orElse(null);
//                if (table == null) {
//                    return indexes;
//                }
//                String order = query.substring(orderPos + 9);
//                addIndexesForSubsequentColumns(indexes, order, table);
//            }
            List<Table> tables = new ArrayList<>(tableJoins.keySet());
            for (Table table : tables) {
                indexes.put(table, new ArrayList<>());
            }
            List<Pair<Table, String>> orderTableCols = new ArrayList<>();
            String order = query.substring(orderPos + 9);
            boolean sortAsc = false;
            boolean sortDesc = false;
            Table firstTable = null;
            boolean tablesMixed = false;
            for (String orderPart : order.split(",")) {
                String[] colOrder = orderPart.split(" ");
                Pair<Table, String> tableCol = tableForColumn(colOrder[0], tables);
                if (tableCol != null) {
                    Table table = tableCol.getLeft();
                    if (firstTable == null) {
                        firstTable = table;
                    } else if (!firstTable.equals(table)) {
                        addIndexesForJoins(indexes, tableJoins);
                        tablesMixed = true;
                        break;
                    }
                    orderTableCols.add(tableCol);
                    if (colOrder.length > 1 && "desc".equals(colOrder[1])) {
                        sortDesc = true;
                    } else {
                        sortAsc = true;
                    }
                }
                if (sortAsc && sortDesc) {
                    break;
                }
            }
            if (!(sortAsc && sortDesc) && !tablesMixed) {
                for (Pair<Table, String> tableCol : orderTableCols) {
                    String column = tableCol.getRight();
                    List<String> indexCols = indexes.get(tableCol.getLeft());
                    if (!indexCols.contains(column)) {
                        indexCols.add(column);
                    }
                }
            }
            for (Map.Entry<Table, List<String>> entry : indexes.entrySet()) {
                List<String> indexCols = entry.getValue();
                if (indexCols.isEmpty()) {
                    tableJoins.get(entry.getKey()).stream().findAny().ifPresent(indexCols::add);
                }
            }
            indexes.entrySet().removeIf(entry -> entry.getValue().isEmpty());
        } else {
            String from = query.substring(fromPos + 4);
            Map<Table, Set<String>> tableJoins = tableJoins(from);
            addIndexesForJoins(indexes, tableJoins);
        }
        return indexes;
    }

    private boolean isConstExpression(String expression) {
        return Arrays.stream(CONST_EXPRESSIONS).anyMatch(constExpression -> constExpression.equals(expression));
    }

//    private void addIndexesForSubsequentColumns(Map<Table, List<String>> indexes, String columns, Table table) {
//        List<String> indexCols = Arrays.stream(columns.split(","))
//                .map(col -> {
//                    for (String column : table.getColumns()) {
//                        if (column.equalsIgnoreCase(col)) {
//                            return column;
//                        }
//                    }
//                    return col;
//                })
//                .collect(Collectors.toList());
//        indexes.put(table, indexCols);
//    }

    private void addIndexesForJoins(Map<Table, List<String>> indexes, Map<Table, Set<String>> tableJoins) {
        for (Map.Entry<Table, Set<String>> entry : tableJoins.entrySet()) {
            for (String column : entry.getValue()) {
                List<String> columns = new ArrayList<>();
                columns.add(column);
                indexes.put(entry.getKey(), columns);
            }
        }
    }

    private Map<Table, Set<String>> tableJoins(String from) {
        Map<Table, Set<String>> tableJoins = new HashMap<>();
        from = StringUtils.trim(from);
        int joinPos = pos(from, "join");
        if (joinPos < 0) {
            Table table = tableFromName(from, tables);
            if (table != null) {
                tableJoins.put(table, Collections.emptySet());
            }
        } else {
            String[] joinSplit = from.split(" join ");
            Table tableFromName = tableFromName(joinSplit[0], tables);
            if (tableFromName != null) {
                tableJoins.put(tableFromName, new HashSet<>());
            }
            for (int i = 1; i < joinSplit.length; i++) {
                String[] onSplit = joinSplit[i].split(" on ");
                Table t = tableFromName(onSplit[0], tables);
                if (t != null) {
                    tableJoins.put(t, new HashSet<>());
                }
                String[] eqSplit = onSplit[1].split(" = ");
                for (String col : eqSplit) {
                    Pair<Table, String> tableCol = tableForColumn(col, tables);
                    if (tableCol != null) {
                        tableJoins.get(tableCol.getLeft()).add(tableCol.getRight());
                    }
                }
            }
        }
        return tableJoins;
    }

    @Nullable
    private Pair<Table, String> tableForColumn(String col, List<Table> tables) {
        if (col.contains(".")) {
            String[] dotSplit = col.split("\\.");
            Table table = tableFromName(dotSplit[0], this.tables);
            if (table == null) {
                return null;
            }
            for (String column : table.getColumns()) {
                if (column.equalsIgnoreCase(dotSplit[1])) {
                    return new ImmutablePair<>(table, column);
                }
            }
        } else {
            for (Table table : tables) {
                for (String column : table.getColumns()) {
                    if (col.equalsIgnoreCase(column)) {
                        return new ImmutablePair<>(table, column);
                    }
                }
            }
        }
        return null;
    }

    private int pos(@NotNull String str, String substring) {
        int i = str.indexOf(substring);
        while (i >= 0) {
            boolean separated = true;
            if (i > 0) {
                char charBefore = str.charAt(i - 1);
                if (charBefore != ' ') {
                    separated = false;
                }
            }
            if (!separated) {
                i = str.indexOf(substring, i);
                continue;
            }
            if (i + substring.length() < str.length() - 1) {
                char charAfter = str.charAt(i + substring.length());
                if (charAfter != ' ') {
                    separated = false;
                }
            }
            if (separated) {
                return i;
            }
            i = str.indexOf(substring + 1, i);
        }
        return i;
    }

    @Nullable
    private Table tableFromName(String name, List<Table> tables) {
        for (Table table : tables) {
            if (table.getName().equalsIgnoreCase(name)) {
                return table;
            }
        }
        return null;
    }
}
