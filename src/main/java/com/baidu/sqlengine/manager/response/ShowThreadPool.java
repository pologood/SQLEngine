
package com.baidu.sqlengine.manager.response;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.util.IntegerUtil;
import com.baidu.sqlengine.util.LongUtil;
import com.baidu.sqlengine.util.tool.NameableExecutor;
import com.baidu.sqlengine.util.StringUtil;

/**
 * 查看线程池状态
 *
 */
public final class ShowThreadPool {

	private static final int FIELD_COUNT = 6;
	private static final ResultSetHeaderPacket header = PacketUtil
			.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("POOL_SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE_COUNT", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TASK_QUEUE_SIZE",
				Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("COMPLETED_TASK",
				Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TOTAL_TASK",
				Fields.FIELD_TYPE_LONGLONG);
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
		List<NameableExecutor> executors = getExecutors();
		for (NameableExecutor exec : executors) {
			if (exec != null) {
				RowDataPacket row = getRow(exec, c.getCharset());
				row.packetId = ++packetId;
				buffer = row.write(buffer, c, true);
			}
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// write buffer
		c.write(buffer);
	}

	private static RowDataPacket getRow(NameableExecutor exec, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(exec.getName(), charset));
		row.add(IntegerUtil.toBytes(exec.getPoolSize()));
		row.add(IntegerUtil.toBytes(exec.getActiveCount()));
		row.add(IntegerUtil.toBytes(exec.getQueue().size()));
		row.add(LongUtil.toBytes(exec.getCompletedTaskCount()));
		row.add(LongUtil.toBytes(exec.getTaskCount()));
		return row;
	}

	private static List<NameableExecutor> getExecutors() {
		List<NameableExecutor> list = new LinkedList<NameableExecutor>();
		SqlEngineServer server = SqlEngineServer.getInstance();
		list.add(server.getTimerExecutor());
		list.add(server.getBusinessExecutor());

		return list;
	}
}