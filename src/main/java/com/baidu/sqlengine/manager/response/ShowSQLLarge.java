package com.baidu.sqlengine.manager.response;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.statistic.stat.UserSqlLargeStat;
import com.baidu.sqlengine.statistic.stat.UserStat;
import com.baidu.sqlengine.statistic.stat.UserStatAnalyzer;
import com.baidu.sqlengine.util.LongUtil;
import com.baidu.sqlengine.util.StringUtil;

/**
 * 查询每个用户大集合返回的 SQL
 *
 */
public class ShowSQLLarge {

	 private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;
        
        fields[i] = PacketUtil.getField("ROWS", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("START_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("EXECUTE_TIME", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c, boolean isClear) {
        ByteBuffer buffer = c.allocate();

        // write header
        buffer = header.write(buffer, c,true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c,true);
        }

        // write eof
        buffer = eof.write(buffer, c,true);

        // write rows
        byte packetId = eof.packetId;        
        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
        	String user = userStat.getUser();

            List<UserSqlLargeStat.SqlLarge> sqls = userStat.getSqlLargeRowStat().getSqls();
            for (UserSqlLargeStat.SqlLarge sql : sqls) {
                if (sql != null) {
                    RowDataPacket row = getRow(user, sql, c.getCharset());
                    row.packetId = ++packetId;
                    buffer = row.write(buffer, c,true);
                }
            }
            
            if ( isClear ) {
            	userStat.getSqlLargeRowStat().clear();//读取大结果集SQL后，清理
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String user, com.baidu.sqlengine.statistic.stat.UserSqlLargeStat.SqlLarge sql, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add( StringUtil.encode(user, charset) );
        row.add( LongUtil.toBytes( sql.getSqlRows() ) );
        row.add( LongUtil.toBytes(sql.getStartTime() ) );
        row.add( LongUtil.toBytes(sql.getExecuteTime() ) );
        row.add( StringUtil.encode(sql.getSql(), charset) );
        return row;
    }
}

