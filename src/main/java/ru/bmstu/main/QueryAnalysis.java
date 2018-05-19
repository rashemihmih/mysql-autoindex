package ru.bmstu.main;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryAnalysis {

    public QueryAnalysis(String query) {
        query = query.toLowerCase();
        Map<Integer, List<Integer>> depthToSelectsStarts = depthToSelectStarts(query);
        Map<Integer, List<Pair<Integer, Integer>>> depthToBounds = depthToBounds(query, depthToSelectsStarts);
        List<String> simpleQueries = simpleQueries(query, depthToBounds);
        System.out.println();
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
                    if (depth < 0 || j == query.length() - 1) {
                        end = j;
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
}
