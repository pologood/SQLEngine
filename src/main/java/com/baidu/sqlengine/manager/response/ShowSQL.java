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
import com.baidu.sqlengine.statistic.stat.UserSqlLastStat;
import com.baidu.sqlengine.statistic.stat.UserStat;
import com.baidu.sqlengine.statistic.stat.UserStatAnalyzer;

import com.baidu.sqlengine.util.LongUtil;
import com.baidu.sqlengine.util.StringUtil;

/**
 * 查询用户最近执行的SQL记录
 */
public final class ShowSQL {

    private static final int FIELD_COUNT = 5;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();

    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("ID", Fields.FIELD_TYPE_LONGLONG);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("USER", Fields.FIELD_TYPE_VARCHAR);
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
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            buffer = field.write(buffer, c, true);
        }

        // write eof
        buffer = eof.write(buffer, c, true);

        // write rows
        byte packetId = eof.packetId;
        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            String user = userStat.getUser();
            List<UserSqlLastStat.SqlLast> sqls = userStat.getSqlLastStat().getSqls();
            int i = 1;
            for (UserSqlLastStat.SqlLast sqlLast : sqls) {
                if (sqlLast != null) {
                    RowDataPacket row = getRow(user, sqlLast, i, c.getCharset());
                    row.packetId = ++packetId;
                    i++;
                    buffer = row.write(buffer, c, true);
                }
            }

            //读取SQL监控后清理
            if (isClear) {
                userStat.getSqlLastStat().clear();
            }
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // write buffer
        c.write(buffer);
    }

    private static RowDataPacket getRow(String user, UserSqlLastStat.SqlLast sql, int idx, String charset) {

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(LongUtil.toBytes(idx));
        row.add(StringUtil.encode(user, charset));
        row.add(LongUtil.toBytes(sql.getStartTime()));
        row.add(LongUtil.toBytes(sql.getExecuteTime()));
        row.add(StringUtil.encode(sql.getSql(), charset));
        return row;
    }

}