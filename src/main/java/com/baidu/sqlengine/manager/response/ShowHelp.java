package com.baidu.sqlengine.manager.response;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.util.StringUtil;

/**
 * 打印SqlEngineServer所支持的语句
 */
public final class ShowHelp {

    private static final int FIELD_COUNT = 2;
    private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
    private static final EOFPacket eof = new EOFPacket();
    static {
        int i = 0;
        byte packetId = 0;
        header.packetId = ++packetId;

        fields[i] = PacketUtil.getField("STATEMENT", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        fields[i] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
        fields[i++].packetId = ++packetId;

        eof.packetId = ++packetId;
    }

    public static void execute(ManagerConnection c) {
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
        for (String key : keys) {
            RowDataPacket row = getRow(key, helps.get(key), c.getCharset());
            row.packetId = ++packetId;
            buffer = row.write(buffer, c,true);
        }

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c,true);

        // post write
        c.write(buffer);
    }

    private static RowDataPacket getRow(String stmt, String desc, String charset) {
        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(stmt, charset));
        row.add(StringUtil.encode(desc, charset));
        return row;
    }

    private static final Map<String, String> helps = new LinkedHashMap<String, String>();
    private static final List<String> keys = new LinkedList<String>();
    static {
        // show
        helps.put("show @@version", "Report SqlEngine Server version");
        helps.put("show @@server", "Report server status");
        helps.put("show @@threadpool", "Report threadPool status");
        helps.put("show @@database", "Report databases");
        helps.put("show @@datanode", "Report dataNodes");
        helps.put("show @@datanode where schema = ?", "Report dataNodes");
        helps.put("show @@datasource", "Report dataSources");
        helps.put("show @@datasource where dataNode = ?", "Report dataSources");
        helps.put("show @@processor", "Report processor status");
        helps.put("show @@connection", "Report connection status");
        helps.put("show @@cache", "Report system cache usage");
        helps.put("show @@backend", "Report backend connection status");
        helps.put("show @@session", "Report front session details");
        helps.put("show @@sql.high", "Report Hight Frequency SQL");
        helps.put("show @@sql.slow", "Report slow SQL");
        helps.put("show @@slow where schema = ?", "Report schema slow sql");
        helps.put("show @@slow where datanode = ?", "Report datanode slow sql");
        // kill
        helps.put("kill @@connection id1,id2,...", "Kill the specified connections");
        // stop
        helps.put("stop @@heartbeat name:time", "Pause dataNode heartbeat");
        // reload
        helps.put("reload @@config", "Reload basic config from file");
        // rollback
        helps.put("rollback @@config", "Rollback all config from memory");
        // offline/online
        helps.put("offline", "Change SqlEngine status to OFF");
        helps.put("online", "Change SqlEngine status to ON");
        // clear
        helps.put("clear @@slow where schema = ?", "Clear slow sql by schema");
        helps.put("clear @@slow where datanode = ?", "Clear slow sql by datanode");
        // list sort
        keys.addAll(helps.keySet());
    }

}