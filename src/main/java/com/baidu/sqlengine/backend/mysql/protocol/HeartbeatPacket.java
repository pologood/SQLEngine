package com.baidu.sqlengine.backend.mysql.protocol;

import java.nio.ByteBuffer;

import com.baidu.sqlengine.backend.mysql.BufferUtil;
import com.baidu.sqlengine.backend.mysql.MySQLMessage;
import com.baidu.sqlengine.net.BackendAIOConnection;

/**
 * From client to server when the client do heartbeat between sqlEngine cluster.
 * <p/>
 * <pre>
 * Bytes         Name
 * -----         ----
 * 1             command
 * n             id
 */
public class HeartbeatPacket extends MySQLPacket {

    public byte command;
    public long id;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        id = mm.readLength();
    }

    @Override
    public void write(BackendAIOConnection c) {
        ByteBuffer buffer = c.allocate();
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        buffer.put(command);
        BufferUtil.writeLength(buffer, id);
        c.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return 1 + BufferUtil.getLength(id);
    }

    @Override
    protected String getPacketInfo() {
        return "SqlEngine Heartbeat Packet";
    }

}