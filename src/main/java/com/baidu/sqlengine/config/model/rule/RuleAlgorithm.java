package com.baidu.sqlengine.config.model.rule;

public interface RuleAlgorithm {

    /**
     * init
     *
     * @param
     */
    void init();

    /**
     * return sharding nodes's id
     * columnValue is column's value
     *
     * @return never null
     */
    Integer calculate(String columnValue);

    Integer[] calculateRange(String beginValue, String endValue);
}