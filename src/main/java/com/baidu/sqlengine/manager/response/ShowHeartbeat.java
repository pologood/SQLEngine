package com.baidu.sqlengine.manager.response;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.backend.datasource.PhysicalDatasource;
import com.baidu.sqlengine.backend.heartbeat.DBHeartbeat;
import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.util.IntegerUtil;
import com.baidu.sqlengine.util.LongUtil;

public class ShowHeartbeat {

	private static final int FIELD_COUNT = 11;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("HOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("PORT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RS_CODE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RETRY", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("STATUS", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TIMEOUT", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("EXECUTE_TIME",Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("LAST_ACTIVE_TIME",Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("STOP", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void response(ManagerConnection c) {
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
		for (RowDataPacket row : getRows()) {
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

	private static List<RowDataPacket> getRows() {
		List<RowDataPacket> list = new LinkedList<RowDataPacket>();
		SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
		// host nodes
		Map<String, PhysicalDBPool> dataHosts = conf.getDataHosts();
		for (PhysicalDBPool pool : dataHosts.values()) {
			for (PhysicalDatasource ds : pool.getAllDataSources()) {
				DBHeartbeat hb = ds.getHeartbeat();
				RowDataPacket row = new RowDataPacket(FIELD_COUNT);
				row.add(ds.getName().getBytes());
				row.add(ds.getConfig().getDbType().getBytes());
				if (hb != null) {
					row.add(ds.getConfig().getIp().getBytes());
					row.add(IntegerUtil.toBytes(ds.getConfig().getPort()));
					row.add(IntegerUtil.toBytes(hb.getStatus()));
					row.add(IntegerUtil.toBytes(hb.getErrorCount()));
					row.add(hb.isChecking() ? "checking".getBytes() : "idle".getBytes());
					row.add(LongUtil.toBytes(hb.getTimeout()));
					row.add(hb.getRecorder().get().getBytes());
					String lat = hb.getLastActiveTime();
					row.add(lat == null ? null : lat.getBytes());
					row.add(hb.isStop() ? "true".getBytes() : "false".getBytes());
				} else {
					row.add(null);
					row.add(null);
					row.add(null);
					row.add(null);
					row.add(null);
					row.add(null);
					row.add(null);
					row.add(null);
					row.add(null);
				}
				list.add(row);
			}
		}
		return list;
	}

}