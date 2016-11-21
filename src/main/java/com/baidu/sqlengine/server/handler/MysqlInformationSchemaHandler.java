package com.baidu.sqlengine.server.handler;

import java.nio.ByteBuffer;

import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.util.SchemaUtil;

/**
 * 如：SELECT * FROM information_schema.CHARACTER_SETS 等相关语句进行模拟返回
 */
public class MysqlInformationSchemaHandler {

    /**
     * 写入数据包
     *
     * @param field_count
     * @param fields
     * @param c
     */
    private static void doWrite(int field_count, FieldPacket[] fields, ServerConnection c) {

        ByteBuffer buffer = c.allocate();

        // write header
        ResultSetHeaderPacket header = PacketUtil.getHeader(field_count);
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

        // write last eof
        EOFPacket lastEof = new EOFPacket();
        lastEof.packetId = ++packetId;
        buffer = lastEof.write(buffer, c, true);

        // post write
        c.write(buffer);

    }

    public static void handle(String sql, ServerConnection c) {

        SchemaUtil.SchemaInfo schemaInfo = SchemaUtil.parseSchema(sql);
        if (schemaInfo != null) {

            if (schemaInfo.table.toUpperCase().equals("CHARACTER_SETS")) {

                //模拟列头
                int field_count = 4;
                FieldPacket[] fields = new FieldPacket[field_count];
                fields[0] = PacketUtil.getField("CHARACTER_SET_NAME", Fields.FIELD_TYPE_VAR_STRING);
                fields[1] = PacketUtil.getField("DEFAULT_COLLATE_NAME", Fields.FIELD_TYPE_VAR_STRING);
                fields[2] = PacketUtil.getField("DESCRIPTION", Fields.FIELD_TYPE_VAR_STRING);
                fields[3] = PacketUtil.getField("MAXLEN", Fields.FIELD_TYPE_LONG);

                doWrite(field_count, fields, c);

            } else {
                c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
            }

        } else {
            c.write(c.writeToBuffer(OkPacket.OK, c.allocate()));
        }
    }
}