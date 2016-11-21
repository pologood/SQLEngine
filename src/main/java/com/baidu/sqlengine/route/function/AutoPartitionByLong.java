
package com.baidu.sqlengine.route.function;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;

import com.baidu.sqlengine.config.model.rule.RuleAlgorithm;
import com.baidu.sqlengine.util.NumberParseUtil;

/**
 * auto partition by Long ,can be used in auto increment primary key partition
 */
public class AutoPartitionByLong extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private String mapFile;
    private LongRange[] longRongs;

    private int defaultNode = -1;

    @Override
    public void init() {

        initialize();
    }

    public void setMapFile(String mapFile) {
        this.mapFile = mapFile;
    }

    @Override
    public Integer calculate(String columnValue) {
        //		columnValue = NumberParseUtil.eliminateQoute(columnValue);
        try {
            long value = Long.parseLong(columnValue);
            Integer rst = null;
            for (LongRange longRang : this.longRongs) {
                if (value <= longRang.valueEnd && value >= longRang.valueStart) {
                    return longRang.nodeIndx;
                }
            }
            //数据超过范围，暂时使用配置的默认节点
            if (rst == null && defaultNode >= 0) {
                return defaultNode;
            }
            return rst;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue)
                    .append(" Please eliminate any quote and non number within it.").toString(), e);
        }
    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        return AbstractPartitionAlgorithm.calculateSequenceRange(this, beginValue, endValue);
    }

    @Override
    public int getPartitionNum() {
        int nPartition = longRongs.length;
        return nPartition;
    }

    private void initialize() {
        BufferedReader in = null;
        try {
            // FileInputStream fin = new FileInputStream(new File(fileMapPath));
            InputStream fin = this.getClass().getClassLoader()
                    .getResourceAsStream(mapFile);
            if (fin == null) {
                throw new RuntimeException("can't find class resource file "
                        + mapFile);
            }
            in = new BufferedReader(new InputStreamReader(fin));
            LinkedList<LongRange> longRangeList = new LinkedList<LongRange>();

            for (String line = null; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//")) {
                    continue;
                }
                int ind = line.indexOf('=');
                if (ind < 0) {
                    System.out.println(" warn: bad line int " + mapFile + " :"
                            + line);
                    continue;
                }
                String pairs[] = line.substring(0, ind).trim().split("-");
                long longStart = NumberParseUtil.parseLong(pairs[0].trim());
                long longEnd = NumberParseUtil.parseLong(pairs[1].trim());
                int nodeId = Integer.parseInt(line.substring(ind + 1)
                        .trim());
                longRangeList
                        .add(new LongRange(nodeId, longStart, longEnd));

            }
            longRongs = longRangeList.toArray(new LongRange[longRangeList
                    .size()]);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }

        } finally {
            try {
                in.close();
            } catch (Exception e2) {
            }
        }
    }

    public int getDefaultNode() {
        return defaultNode;
    }

    public void setDefaultNode(int defaultNode) {
        this.defaultNode = defaultNode;
    }

    static class LongRange {
        public final int nodeIndx;
        public final long valueStart;
        public final long valueEnd;

        public LongRange(int nodeIndx, long valueStart, long valueEnd) {
            super();
            this.nodeIndx = nodeIndx;
            this.valueStart = valueStart;
            this.valueEnd = valueEnd;
        }

    }
}