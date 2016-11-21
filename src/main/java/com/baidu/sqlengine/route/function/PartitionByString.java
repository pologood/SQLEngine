
package com.baidu.sqlengine.route.function;

import com.baidu.sqlengine.config.model.rule.RuleAlgorithm;
import com.baidu.sqlengine.util.tool.Pair;
import com.baidu.sqlengine.route.util.PartitionUtil;
import com.baidu.sqlengine.util.StringUtil;

public final class PartitionByString extends AbstractPartitionAlgorithm implements RuleAlgorithm  {
  
    private int hashSliceStart = 0;
    /** 0 means str.length(), -1 means str.length()-1 */
    private int hashSliceEnd = 8;
    protected int[] count;
    protected int[] length;
    protected PartitionUtil partitionUtil;

    public void setPartitionCount(String partitionCount) {
        this.count = toIntArray(partitionCount);
    }

    public void setPartitionLength(String partitionLength) {
        this.length = toIntArray(partitionLength);
    }


	public void setHashLength(int hashLength) {
        setHashSlice(String.valueOf(hashLength));
    }

    public void setHashSlice(String hashSlice) {
        Pair<Integer, Integer> p = sequenceSlicing(hashSlice);
        hashSliceStart = p.getKey();
        hashSliceEnd = p.getValue();
    }


    /**
     * "2" -&gt; (0,2)<br/>
     * "1:2" -&gt; (1,2)<br/>
     * "1:" -&gt; (1,0)<br/>
     * "-1:" -&gt; (-1,0)<br/>
     * ":-1" -&gt; (0,-1)<br/>
     * ":" -&gt; (0,0)<br/>
     */
    public static Pair<Integer, Integer> sequenceSlicing(String slice) {
        int ind = slice.indexOf(':');
        if (ind < 0) {
            int i = Integer.parseInt(slice.trim());
            if (i >= 0) {
                return new Pair<Integer, Integer>(0, i);
            } else {
                return new Pair<Integer, Integer>(i, 0);
            }
        }
        String left = slice.substring(0, ind).trim();
        String right = slice.substring(1 + ind).trim();
        int start, end;
        if (left.length() <= 0) {
            start = 0;
        } else {
            start = Integer.parseInt(left);
        }
        if (right.length() <= 0) {
            end = 0;
        } else {
            end = Integer.parseInt(right);
        }
        return new Pair<Integer, Integer>(start, end);
    }

	@Override
	public void init() {
		partitionUtil = new PartitionUtil(count,length);
		
	}
	private static int[] toIntArray(String string) {
		String[] strs = com.baidu.sqlengine.util.SplitUtil.split(string, ',', true);
		int[] ints = new int[strs.length];
		for (int i = 0; i < strs.length; ++i) {
			ints[i] = Integer.parseInt(strs[i]);
		}
		return ints;
	}
	@Override
	public Integer calculate(String key) {
        int start = hashSliceStart >= 0 ? hashSliceStart : key.length() + hashSliceStart;
        int end = hashSliceEnd > 0 ? hashSliceEnd : key.length() + hashSliceEnd;
        long hash = StringUtil.hash(key, start, end);
        return partitionUtil.partition(hash);
	}

	@Override
	public int getPartitionNum() {
		int nPartition = 0;
		for(int i = 0; i < count.length; i++) {
			nPartition += count[i];
		}
		return nPartition;
	}
	
}