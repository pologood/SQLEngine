package com.baidu.sqlengine.manager.response;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.datasource.PhysicalDBNode;
import com.baidu.sqlengine.backend.datasource.PhysicalDBPool;
import com.baidu.sqlengine.backend.datasource.PhysicalDatasource;
import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.config.SqlEngineConfig;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.manager.ManagerConnection;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.util.tool.Pair;
import com.baidu.sqlengine.util.PairUtil;
import com.baidu.sqlengine.util.IntegerUtil;
import com.baidu.sqlengine.util.LongUtil;
import com.baidu.sqlengine.util.StringUtil;
import com.baidu.sqlengine.util.TimeUtil;

/**
 * 查看数据节点信息
 */
public final class ShowDataNode {

	private static final NumberFormat nf = DecimalFormat.getInstance();
	private static final int FIELD_COUNT = 12;
	private static final ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	private static final EOFPacket eof = new EOFPacket();
	static {
		nf.setMaximumFractionDigits(3);

		int i = 0;
		byte packetId = 0;
		header.packetId = ++packetId;

		fields[i] = PacketUtil.getField("NAME", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("DATHOST", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("INDEX", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TYPE", Fields.FIELD_TYPE_VAR_STRING);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("ACTIVE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("IDLE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("SIZE", Fields.FIELD_TYPE_LONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("EXECUTE", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("TOTAL_TIME", Fields.FIELD_TYPE_DOUBLE);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("MAX_TIME", Fields.FIELD_TYPE_DOUBLE);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("MAX_SQL", Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		fields[i] = PacketUtil.getField("RECOVERY_TIME",
				Fields.FIELD_TYPE_LONGLONG);
		fields[i++].packetId = ++packetId;

		eof.packetId = ++packetId;
	}

	public static void execute(ManagerConnection c, String name) {
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
		SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
		Map<String, PhysicalDBNode> dataNodes = conf.getDataNodes();
		List<String> keys = new ArrayList<String>();
		if (StringUtil.isEmpty(name)) {
			keys.addAll(dataNodes.keySet());
		} else {
			SchemaConfig sc = conf.getSchemas().get(name);
			if (null != sc) {
				keys.addAll(sc.getAllDataNodes());
			}
		}
		Collections.sort(keys, new Comparators<String>());
		for (String key : keys) {
			RowDataPacket row = getRow(dataNodes.get(key), c.getCharset());
			row.packetId = ++packetId;
			buffer = row.write(buffer, c, true);
		}

		// write last eof
		EOFPacket lastEof = new EOFPacket();
		lastEof.packetId = ++packetId;
		buffer = lastEof.write(buffer, c, true);

		// post write
		c.write(buffer);
	}

	private static RowDataPacket getRow(PhysicalDBNode node, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(node.getName(), charset));
		row.add(StringUtil.encode(
				node.getDbPool().getHostName() + '/' + node.getDatabase(),
				charset));
		PhysicalDBPool pool = node.getDbPool();
		PhysicalDatasource ds = pool.getSource();
		if (ds != null) {
			int active = ds.getActiveCountForSchema(node.getDatabase());
			int idle = ds.getIdleCountForSchema(node.getDatabase());
			row.add(IntegerUtil.toBytes(pool.getActivedIndex()));
			row.add(StringUtil.encode(ds.getConfig().getDbType(), charset));
			row.add(IntegerUtil.toBytes(active));
			row.add(IntegerUtil.toBytes(idle));
			row.add(IntegerUtil.toBytes(ds.getSize()));
		} else {
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
			row.add(null);
		}
		row.add(LongUtil.toBytes(ds.getExecuteCountForSchema(node.getDatabase())));
		row.add(StringUtil.encode(nf.format(0), charset));
		row.add(StringUtil.encode(nf.format(0), charset));
		row.add(LongUtil.toBytes(0));
		long recoveryTime = pool.getSource().getHeartbeatRecoveryTime()
				- TimeUtil.currentTimeMillis();
		row.add(LongUtil.toBytes(recoveryTime > 0 ? recoveryTime / 1000L : -1L));
		return row;
	}

	private static final class Comparators<T> implements Comparator<String> {
		@Override
		public int compare(String s1, String s2) {
			Pair<String, Integer> p1 = PairUtil.splitIndex(s1, '[', ']');
			Pair<String, Integer> p2 = PairUtil.splitIndex(s2, '[', ']');
			if (p1.getKey().compareTo(p2.getKey()) == 0) {
				return p1.getValue() - p2.getValue();
			} else {
				return p1.getKey().compareTo(p2.getKey());
			}
		}
	}

}