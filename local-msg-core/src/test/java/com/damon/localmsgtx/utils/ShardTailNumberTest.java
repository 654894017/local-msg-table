package com.damon.localmsgtx.utils;

import org.junit.Test;

import java.util.List;


public class ShardTailNumberTest {

    @Test
    public void testGenerateTailNumbers() {
        ShardTailNumber shardTailNumber = new ShardTailNumber(10,
                9, 2);
        List<String> tailNumbers = shardTailNumber.generateTailNumbers();
        System.out.println(tailNumbers);
    }

}