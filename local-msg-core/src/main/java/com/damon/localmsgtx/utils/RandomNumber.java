package com.damon.localmsgtx.utils;

import java.text.DecimalFormat;

/**
 * 随机数字字符串生成器
 * <p>
 * 生成指定位数的随机数字字符串，位数不足时前补零。
 * 用于生成消息的随机因子（分片路由使用）。
 */
public class RandomNumber {

    private final int length;

    public RandomNumber(int length) {
        this.length = length;
    }

    /**
     * 生成指定长度的随机数字字符串
     *
     * @return 指定长度的随机数字字符串（如length=4，返回"0000"~"9999"）
     * @throws IllegalArgumentException 如果长度小于等于0
     */
    public String generate() {
        if (length <= 0) {
            throw new IllegalArgumentException("长度必须大于0");
        }
        StringBuilder formatBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            formatBuilder.append('0');
        }
        DecimalFormat df = new DecimalFormat(formatBuilder.toString());
        int maxValue = (int) Math.pow(10, length);
        int randomNumber = (int) (Math.random() * maxValue);
        return df.format(randomNumber);
    }
}
