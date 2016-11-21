package com.baidu.sqlengine.merge;

/**
 * column ->node index
 * 
 */
public class ColumnRoutePair {
	public final String colValue;
	public final RangeValue rangeValue;
	public Integer nodeId;

	public int slot=-2;

	public int getSlot() {
		return slot;
	}

	public void setSlot(int slot) {
		this.slot = slot;
	}

	public ColumnRoutePair(String colValue) {
		super();
		this.colValue = colValue;
		this.rangeValue = null;
	}

	public ColumnRoutePair(RangeValue rangeValue) {
		super();
		this.rangeValue = rangeValue;
		this.colValue = null;
	}

	public Integer getNodeId() {
		return nodeId;
	}

	public void setNodeId(Integer nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((colValue == null) ? 0 : colValue.hashCode());
		result = prime * result + ((rangeValue == null) ? 0 : rangeValue.hashCode());
		result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		ColumnRoutePair other = (ColumnRoutePair) obj;
		if (colValue == null) {
			if (other.colValue != null) {
				return false;
			}
		} else if (!colValue.equals(other.colValue)) {
			return false;
		}

		if (rangeValue == null) {
			if (other.rangeValue != null) {
				return false;
			}
		} else if (!rangeValue.equals(other.rangeValue)) {
			return false;
		}

		if (nodeId == null) {
			if (other.nodeId != null) {
				return false;
			}
		} else if (!nodeId.equals(other.nodeId)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "ColumnRoutePair [colValue=" + colValue + ", nodeId=" + nodeId + "]";
	}
}
