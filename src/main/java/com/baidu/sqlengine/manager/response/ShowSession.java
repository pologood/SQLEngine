package com.baidu.sqlengine.manager.response;

import java.nio.ByteBuffer;
import java.util.Collection;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.BackendConnection;
import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.net.FrontendConnection;
import com.baidu.sqlengine.net.NIOProcessor;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.server.NonBlockingSession;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.util.StringUtil;

/**
 * show front session detail info
 * 
 */
public class ShowSession {
	private static final int FIELD_COUNT = 3;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("SESSION", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("DN_COUNT", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("DN_LIST", Fields.FIELD_TYPE_VARCHAR);
		fields[i++].packetId = ++packetId;
		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c) {
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
		for (NIOProcessor process : SqlEngineServer.getInstance().getProcessors()) {
			for (FrontendConnection front : process.getFrontends().values()) {

				if (!(front instanceof ServerConnection)) {
					continue;
				}
				ServerConnection sc = (ServerConnection) front;
				RowDataPacket row = getRow(sc, c.getCharset());
				if (row != null) {
					row.packetId = ++packetId;
					buffer = row.write(buffer, c, true);
				}
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}

	private static RowDataPacket getRow(ServerConnection sc, String charset) {
		StringBuilder sb = new StringBuilder();
		NonBlockingSession ssesion = sc.getSession();
		Collection<BackendConnection> backConnections = ssesion.getTargetMap()
				.values();
		int cncount = backConnections.size();
		if (cncount == 0) {
			return null;
		}
		for (BackendConnection backCon : backConnections) {
			sb.append(backCon).append("\r\n");
		}
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(sc.getId() + "", charset));
		row.add(StringUtil.encode(cncount + "", charset));
		row.add(StringUtil.encode(sb.toString(), charset));
		return row;
	}
}
