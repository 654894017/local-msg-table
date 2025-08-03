package com.damon.localmsgtx.utils;

import java.util.ArrayList;
import java.util.List;

public class ListUtils {

    public static <T> List<List<T>> split(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(i + batchSize, list.size());
            batches.add(list.subList(i, end));
        }
        return batches;
    }
}
