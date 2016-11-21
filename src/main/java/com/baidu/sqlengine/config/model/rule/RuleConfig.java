package com.baidu.sqlengine.config.model.rule;

import com.baidu.sqlengine.route.function.AbstractPartitionAlgorithm;

import java.io.Serializable;

/**
 * 分片规则，column是用于分片的数据库物理字段
 */
public class RuleConfig implements Serializable {
	private final String column;
	private final String functionName;
	private AbstractPartitionAlgorithm ruleAlgorithm;

	public RuleConfig(String column, String functionName) {
		if (functionName == null) {
			throw new IllegalArgumentException("functionName is null");
		}
		this.functionName = functionName;
		if (column == null || column.length() <= 0) {
			throw new IllegalArgumentException("no rule column is found");
		}
		this.column = column;
	}

	public AbstractPartitionAlgorithm getRuleAlgorithm() {
		return ruleAlgorithm;
	}

	public void setRuleAlgorithm(AbstractPartitionAlgorithm ruleAlgorithm) {
		this.ruleAlgorithm = ruleAlgorithm;
	}

	/**
	 * @return unmodifiable, upper-case
	 */
	public String getColumn() {
		return column;
	}

	public String getFunctionName() {
		return functionName;
	}

}
