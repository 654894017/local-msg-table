package com.damon.localmsgtx.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * 分片尾号生成器
 * <p>
 * 根据总分片数和当前分片索引，生成当前分片应处理的尾号列表。
 * 用于分布式定时任务的分片路由，每个分片只处理其对应尾号的消息。
 * <p>
 * 示例：总分片3，当前分片0，尾号长度2 → 生成 ["00", "03", "06", ..., "99"]
 */
public class ShardTailNumber {

    /**
     * 总分片数
     */
    private final int shardTotal;

    /**
     * 当前分片索引（0-based）
     */
    private final int shardIndex;

    /**
     * 尾号位数
     */
    private final int tailLength;

    /**
     * 最大尾号值（如尾号长度为2，则最大为99）
     */
    private final int maxTailNumber;

    /**
     * @param shardTotal 总分片数（必须大于0）
     * @param shardIndex 当前分片索引（0-based，必须小于shardTotal）
     * @param tailLength 尾号位数（必须大于0）
     */
    public ShardTailNumber(int shardTotal, int shardIndex, int tailLength) {
        if (shardTotal <= 0 || shardIndex < 0 || shardIndex >= shardTotal || tailLength <= 0) {
            throw new IllegalArgumentException("分片参数不合法");
        }
        this.shardTotal = shardTotal;
        this.shardIndex = shardIndex;
        this.tailLength = tailLength;
        this.maxTailNumber = (int) Math.pow(10, tailLength) - 1;
    }

    /**
     * 生成当前分片负责的尾号列表
     *
     * @return 尾号列表（如 ["00", "03", "06", ...]）
     */
    public List<String> generateTailNumbers() {
        List<String> tailNumbers = new ArrayList<>();
        for (int i = 0; i <= maxTailNumber; i++) {
            if (i % shardTotal == shardIndex) {
                tailNumbers.add(StrUtil.leftPad(String.valueOf(i), tailLength, "0"));
            }
        }
        return tailNumbers;
    }

    public int getShardTotal() {
        return shardTotal;
    }

    public int getShardIndex() {
        return shardIndex;
    }
}
