//package com.baidu.sqlengine.backend.heartbeat;
//
//import com.alibaba.fastjson.JSON;
//import com.baidu.sqlengine.server.interceptor.impl.GlobalTableUtil;
//import com.baidu.sqlengine.sql.SQLJob;
//import com.baidu.sqlengine.sql.SQLQueryResult;
//import com.baidu.sqlengine.sql.SQLQueryResultListener;
//
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import org.apache.commons.lang.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class MySQLConsistencyHelper implements SQLQueryResultListener<SQLQueryResult<Map<String, String>>> {
//	private static final Logger LOGGER = LoggerFactory.getLogger(MySQLConsistencyHelper.class);
//    private MySQLConsistencyChecker heartbeat;
//    private volatile SQLJob sqlJob;
//    private int RETRY_TIMES = 5;
//    private AtomicInteger retryTime = new AtomicInteger();
//
//    public MySQLConsistencyHelper(MySQLConsistencyChecker heartbeat, SQLJob sqlJob) {
//    	this.heartbeat = heartbeat;
//        this.sqlJob = sqlJob;
//        this.retryTime.set(RETRY_TIMES);
//    }
//
//    public MySQLConsistencyHelper(MySQLConsistencyChecker heartbeat,
//    								SQLJob sqlJob, int retryTime) {
//    	this.heartbeat = heartbeat;
//        this.sqlJob = sqlJob;
//        if(retryTime > 0 && retryTime < 10)
//        	this.retryTime.set(retryTime);
//        else
//        	this.retryTime.set(RETRY_TIMES);
//    }
//
//    @Override
//    public void onResult(SQLQueryResult<Map<String, String>> result) {
//    	// {"dataNode":"db2","result":{"max_timestamp":"1450423751170"},"success":true}
//    	// {"dataNode":"db3","result":{"count(*)":"1"},"success":true}
//    	LOGGER.debug("result:" + JSON.toJSONString(result));
//    	Map<String, String> rowMap = null;
//    	String count = null; String innerCol = null;
//    	String maxTimestamp = null;
//    	if(result != null)
//    		rowMap = result.getResult();
//
//    	if(rowMap != null){
//    		maxTimestamp = rowMap.get(GlobalTableUtil.MAX_COLUMN);
//    		count = rowMap.get(GlobalTableUtil.COUNT_COLUMN);
//    		innerCol = rowMap.get(GlobalTableUtil.INNER_COLUMN);
//    		if((rowMap.containsKey(GlobalTableUtil.MAX_COLUMN) && StringUtils.isNotBlank(maxTimestamp))
//    				|| (rowMap.containsKey(GlobalTableUtil.COUNT_COLUMN) && StringUtils.isNotBlank(count))
//    				|| (rowMap.containsKey(GlobalTableUtil.INNER_COLUMN) && StringUtils.isNotBlank(innerCol))){
//    			heartbeat.setResult(result);
//    			return;
//    		}else{
//    			if(this.retryTime.get() > 0){
//					try {
//						TimeUnit.MICROSECONDS.sleep(10);
//					} catch (InterruptedException e) {
//					}
//					this.retryTime.decrementAndGet();
//    				this.sqlJob.run();
//    				return;
//				}
//    			heartbeat.setResult(result);
//    			return;
//    		}
//    	}else{
//    		if(this.retryTime.get() > 0){
//    			try {
//    				TimeUnit.MICROSECONDS.sleep(3);
//				} catch (InterruptedException e) {
//				}
//				this.retryTime.decrementAndGet();
//				this.sqlJob.run();
//				return;
//			}
//    		heartbeat.setResult(result);
//			return;
//    	}
//    }
//
//    public void close(String msg) {
//        SQLJob curJob = sqlJob;
//        if (curJob != null && !curJob.isFinished()) {
//            curJob.teminate(msg);
//            sqlJob = null;
//        }
//    }
//    public MySQLConsistencyChecker getHeartbeat() {
//        return heartbeat;
//    }
//	public SQLJob getSqlJob() {
//		return sqlJob;
//	}
//	public void setSqlJob(SQLJob sqlJob) {
//		this.sqlJob = sqlJob;
//	}
//
//}
