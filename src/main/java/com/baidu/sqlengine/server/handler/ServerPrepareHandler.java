
package com.baidu.sqlengine.server.handler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.mysql.BindValue;
import com.baidu.sqlengine.backend.mysql.ByteUtil;
import com.baidu.sqlengine.backend.mysql.PreparedStatement;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.net.handler.FrontendPrepareHandler;
import com.baidu.sqlengine.backend.mysql.protocol.ExecutePacket;
import com.baidu.sqlengine.backend.mysql.protocol.LongDataPacket;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResetPacket;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.response.PreparedStmtResponse;
import com.baidu.sqlengine.util.HexFormatUtil;

public class ServerPrepareHandler implements FrontendPrepareHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerPrepareHandler.class);
	
    private ServerConnection source;
    private volatile long pstmtId;
    private Map<String, PreparedStatement> pstmtForSql;
    private Map<Long, PreparedStatement> pstmtForId;

    public ServerPrepareHandler(ServerConnection source) {
        this.source = source;
        this.pstmtId = 0L;
        this.pstmtForSql = new HashMap<String, PreparedStatement>();
        this.pstmtForId = new HashMap<Long, PreparedStatement>();
    }

    @Override
    public void prepare(String sql) {
    	
    	LOGGER.debug("use server prepare, sql: " + sql);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForSql.get(sql)) == null) {
        	// 解析获取字段个数和参数个数
        	int columnCount = getColumnCount(sql);
        	int paramCount = getParamCount(sql);
            pstmt = new PreparedStatement(++pstmtId, sql, columnCount, paramCount);
            pstmtForSql.put(pstmt.getStatement(), pstmt);
            pstmtForId.put(pstmt.getId(), pstmt);
        }
        PreparedStmtResponse.response(pstmt, source);
    }
    
    @Override
	public void sendLongData(byte[] data) {
		LongDataPacket packet = new LongDataPacket();
		packet.read(data);
		long pstmtId = packet.getPstmtId();
		PreparedStatement pstmt = pstmtForId.get(pstmtId);
		if(pstmt != null) {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("send long data to prepare sql : " + pstmtForId.get(pstmtId));
			}
			long paramId = packet.getParamId();
			try {
				pstmt.appendLongData(paramId, packet.getLongData());
			} catch (IOException e) {
				source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
			}
		}
	}

	@Override
	public void reset(byte[] data) {
		ResetPacket packet = new ResetPacket();
		packet.read(data);
		long pstmtId = packet.getPstmtId();
		PreparedStatement pstmt = pstmtForId.get(pstmtId);
		if(pstmt != null) {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("reset prepare sql : " + pstmtForId.get(pstmtId));
			}
			pstmt.resetLongData();
			source.write(OkPacket.OK);
		} else {
			source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "can not reset prepare statement : " + pstmtForId.get(pstmtId));
		}
	} 
    
    @Override
    public void execute(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForId.get(pstmtId)) == null) {
            source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pstmtId when executing.");
        } else {
            ExecutePacket packet = new ExecutePacket(pstmt);
            try {
                packet.read(data, source.getCharset());
            } catch (UnsupportedEncodingException e) {
                source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.values;
            // 还原sql中的动态参数为实际参数值
            String sql = prepareStmtBindValue(pstmt, bindValues);
            // 执行sql
            source.getSession().setPrepared(true);
            if(LOGGER.isDebugEnabled()) {
            	LOGGER.debug("execute prepare sql: " + sql);
            }
            source.query( sql );
        }
    }
    
    
    @Override
    public void close(byte[] data) {
    	long pstmtId = ByteUtil.readUB4(data, 5); // 获取prepare stmt id
    	if(LOGGER.isDebugEnabled()) {
    		LOGGER.debug("close prepare stmt, stmtId = " + pstmtId);
    	}
    	PreparedStatement pstmt = pstmtForId.remove(pstmtId);
    	if(pstmt != null) {
    		pstmtForSql.remove(pstmt.getStatement());
    	}
    }
    
    @Override
    public void clear() {
    	this.pstmtForId.clear();
    	this.pstmtForSql.clear();
    }

    private int getColumnCount(String sql) {
    	int columnCount = 0;
    	return columnCount;
    }
    
    // 获取预处理sql中预处理参数个数
    private int getParamCount(String sql) {
    	char[] cArr = sql.toCharArray();
    	int count = 0;
    	for(int i = 0; i < cArr.length; i++) {
    		if(cArr[i] == '?') {
    			count++;
    		}
    	}
    	return count;
    }
    
    /**
     * 组装sql语句,替换动态参数为实际参数值
     * @param pstmt
     * @param bindValues
     * @return
     */
    private String prepareStmtBindValue(PreparedStatement pstmt, BindValue[] bindValues) {
    	String sql = pstmt.getStatement();
    	int paramNumber = pstmt.getParametersNumber();
    	int[] paramTypes = pstmt.getParametersType();
    	for(int i = 0; i < paramNumber; i++) {
    		int paramType = paramTypes[i];
    		BindValue bindValue = bindValues[i];
    		if(bindValue.isNull) {
    			sql = sql.replaceFirst("\\?", "NULL");
    			continue;
    		}
    		switch(paramType & 0xff) {
    		case Fields.FIELD_TYPE_TINY:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.byteBinding));
    			break;
    		case Fields.FIELD_TYPE_SHORT:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.shortBinding));
    			break;
    		case Fields.FIELD_TYPE_LONG:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.intBinding));
    			break;
    		case Fields.FIELD_TYPE_LONGLONG:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.longBinding));
    			break;
    		case Fields.FIELD_TYPE_FLOAT:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.floatBinding));
    			break;
    		case Fields.FIELD_TYPE_DOUBLE:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.doubleBinding));
    			break;
    		case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
            	sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	break;
            case Fields.FIELD_TYPE_TINY_BLOB:
            case Fields.FIELD_TYPE_BLOB:
            case Fields.FIELD_TYPE_MEDIUM_BLOB:
            case Fields.FIELD_TYPE_LONG_BLOB:
            	if(bindValue.value instanceof ByteArrayOutputStream) {
            		byte[] bytes = ((ByteArrayOutputStream) bindValue.value).toByteArray();
            		sql = sql.replaceFirst("\\?", "X'" + HexFormatUtil.bytesToHexString(bytes) + "'");
            	} else {
            		// 正常情况下不会走到else, 除非long data的存储方式(ByteArrayOutputStream)被修改
            		LOGGER.warn("bind value is not a instance of ByteArrayOutputStream, maybe someone change the implement of long data storage!");
            		sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	}
            	break;
            case Fields.FIELD_TYPE_TIME:
            case Fields.FIELD_TYPE_DATE:
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
            	sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	break;
            default:
            	sql = sql.replaceFirst("\\?", bindValue.value.toString());
            	break;
    		}
    	}
    	return sql;
    }

}