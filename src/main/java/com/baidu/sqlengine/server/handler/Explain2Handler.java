package com.baidu.sqlengine.server.handler;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.backend.mysql.nio.handler.SingleNodeHandler;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.route.RouteResultsetNode;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.util.StringUtil;

public class Explain2Handler {

    private static final Logger logger = LoggerFactory.getLogger(Explain2Handler.class);
    private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[1];
    private static final int FIELD_COUNT = 2;
    private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];

    static {
        fields[0] = PacketUtil.getField("SQL",
                Fields.FIELD_TYPE_VAR_STRING);
        fields[1] = PacketUtil.getField("MSG",
                Fields.FIELD_TYPE_VAR_STRING);
    }

    public static void handle(String stmt, ServerConnection c, int offset) {

        try {
            stmt = stmt.substring(offset);
            if (!stmt.toLowerCase().contains("datanode=") || !stmt.toLowerCase().contains("sql=")) {
                showerror(stmt, c, "explain2 datanode=? sql=?");
                return;
            }
            String dataNode = stmt.substring(stmt.indexOf("=") + 1, stmt.indexOf("sql=")).trim();
            String sql = "explain " + stmt.substring(stmt.indexOf("sql=") + 4, stmt.length()).trim();

            if (dataNode == null || dataNode.isEmpty() || sql == null || sql.isEmpty()) {
                showerror(stmt, c, "dataNode or sql is null or empty");
                return;
            }

            RouteResultsetNode node = new RouteResultsetNode(dataNode, ServerParse.SELECT, sql);
            RouteResultSet rrs = new RouteResultSet(sql, ServerParse.SELECT);
            node.setSource(rrs);
            EMPTY_ARRAY[0] = node;
            rrs.setNodes(EMPTY_ARRAY);
            SingleNodeHandler singleNodeHandler = new SingleNodeHandler(rrs, c.getSession());
            singleNodeHandler.execute();
        } catch (Exception e) {
            logger.error(e.getMessage(), e.getCause());
            showerror(stmt, c, e.getMessage());
        }
    }

    private static void showerror(String stmt, ServerConnection c, String msg) {
        ByteBuffer buffer = c.allocate();
        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
        byte packetId = header.packetId;
        buffer = header.write(buffer, c, true);

        // write fields
        for (FieldPacket field : fields) {
            field.packetId = ++packetId;
            buffer = field.write(buffer, c, true);
        }

        // write eof
        EOFPacket eof = new EOFPacket();
        eof.packetId = ++packetId;
        buffer = eof.write(buffer, c, true);

        RowDataPacket row = new RowDataPacket(FIELD_COUNT);
        row.add(StringUtil.encode(stmt, c.getCharset()));
        row.add(StringUtil.encode(msg, c.getCharset()));
        row.packetId = ++packetId;
        buffer = row.write(buffer, c, true);

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);
    }
}
