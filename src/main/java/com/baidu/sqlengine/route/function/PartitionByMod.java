package com.baidu.sqlengine.route.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.baidu.sqlengine.config.model.rule.RuleAlgorithm;

/**
 * number column partion by Mod operator
 * if count is 10 then 0 to 0,21 to 1 (21 % 10 =1)
 */
public class PartitionByMod extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private int count;

    @Override
    public void init() {

    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public Integer calculate(String columnValue) {
        //		columnValue = NumberParseUtil.eliminateQoute(columnValue);
        try {
            BigInteger bigNum = new BigInteger(columnValue).abs();
            return (bigNum.mod(BigInteger.valueOf(count))).intValue();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue)
                    .append(" Please eliminate any quote and non number within it.").toString(), e);
        }
    }

    @Override
    public int getPartitionNum() {
        int nPartition = this.count;
        return nPartition;
    }

}