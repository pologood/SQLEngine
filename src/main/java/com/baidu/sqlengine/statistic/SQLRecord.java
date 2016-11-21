package com.baidu.sqlengine.statistic;

public final class SQLRecord implements Comparable<SQLRecord> {

    public String host;
    public String schema;
    public String statement;
    public long startTime;
    public long executeTime;
    public String dataNode;
    public int dataNodeIndex;

    @Override
    public int compareTo(SQLRecord o) {
        //执行时间从大到小
        long para = o.executeTime - executeTime;
        //开始时间从大到小
        return (int) (para == 0 ? (o.startTime - startTime) : para);
    }

    @Override
    public boolean equals(Object arg0) {
        return super.equals(arg0);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

}