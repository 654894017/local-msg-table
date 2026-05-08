package com.damon.localmsgtx.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * 集合工具类
 */
public class ListUtils {

    /**
     * 将列表按指定大小分割为多个子列表
     *
     * @param list      原始列表
     * @param batchSize 每批大小
     * @param <T>       元素类型
     * @return 分割后的子列表集合
     */
    public static <T> List<List<T>> split(List<T> list, int batchSize) {
        List<List<T>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += batchSize) {
            int end = Math.min(i + batchSize, list.size());
            batches.add(list.subList(i, end));
        }
        return batches;
    }

    public static boolean isEmpty(List list) {
        return list == null || list.isEmpty();
    }

    public static boolean isNotEmpty(List list) {
        return !isEmpty(list);
    }
}
