
package com.baidu.sqlengine.server.handler;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.backend.mysql.PacketUtil;
import com.baidu.sqlengine.constant.ErrorCode;
import com.baidu.sqlengine.constant.Fields;
import com.baidu.sqlengine.config.model.SchemaConfig;
import com.baidu.sqlengine.config.model.SystemConfig;
import com.baidu.sqlengine.config.model.TableConfig;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ResultSetHeaderPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.route.RouteResultSet;
import com.baidu.sqlengine.route.RouteResultsetNode;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.server.util.SchemaUtil;
import com.baidu.sqlengine.util.StringUtil;


public class ExplainHandler {

	private static final Logger logger = LoggerFactory.getLogger(ExplainHandler.class);
    private final static Pattern pattern = Pattern.compile("(?:(\\s*next\\s+value\\s+for\\s*SQL_ENGINESEQ_(\\w+))(,|\\)|\\s)*)+", Pattern.CASE_INSENSITIVE);
	private static final RouteResultsetNode[] EMPTY_ARRAY = new RouteResultsetNode[0];
	private static final int FIELD_COUNT = 2;
	private static final FieldPacket[] fields = new FieldPacket[FIELD_COUNT];
	static {
		fields[0] = PacketUtil.getField("DATA_NODE",
				Fields.FIELD_TYPE_VAR_STRING);
		fields[1] = PacketUtil.getField("SQL", Fields.FIELD_TYPE_VAR_STRING);
	}

	public static void handle(String stmt, ServerConnection c, int offset) {
		stmt = stmt.substring(offset).trim();

		RouteResultSet rrs = getRouteResultset(c, stmt);
		if (rrs == null) {
			return;
		}

		ByteBuffer buffer = c.allocate();

		// write header
		ResultSetHeaderPacket header = PacketUtil.getHeader(FIELD_COUNT);
		byte packetId = header.packetId;
		buffer = header.write(buffer, c,true);

		// write fields
		for (FieldPacket field : fields) {
			field.packetId = ++packetId;
			buffer = field.write(buffer, c,true);
		}

		// write eof
		EOFPacket eof = new EOFPacket();
		eof.packetId = ++packetId;
		buffer = eof.write(buffer, c,true);

		// write rows
		RouteResultsetNode[] rrsn =  rrs.getNodes();
		for (RouteResultsetNode node : rrsn) {
			RowDataPacket row = getRow(node, c.getCharset());
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

	private static RowDataPacket getRow(RouteResultsetNode node, String charset) {
		RowDataPacket row = new RowDataPacket(FIELD_COUNT);
		row.add(StringUtil.encode(node.getName(), charset));
		row.add(StringUtil.encode(node.getStatement().replaceAll("[\\t\\n\\r]", " "), charset));
		return row;
	}

	private static RouteResultSet getRouteResultset(ServerConnection c,
			String stmt) {
		String db = c.getSchema();
        int sqlType = ServerParse.parse(stmt) & 0xff;
		if (db == null) {
            db = SchemaUtil.detectDefaultDb(stmt, sqlType);

            if(db==null)
            {
                c.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return null;
            }
		}
		SchemaConfig schema = SqlEngineServer.getInstance().getConfig()
				.getSchemas().get(db);
		if (schema == null) {
			c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
					+ db + "'");
			return null;
		}
		try {

            if(ServerParse.INSERT==sqlType&&isSqlEngineSeq(stmt, schema))
            {
                c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, "insert sql using sqlEngine seq,you must provide primaryKey value for explain");
                return null;
            }
            SystemConfig system = SqlEngineServer.getInstance().getConfig().getSystem();
            return SqlEngineServer.getInstance().getRouterservice()
					.route(system,schema, sqlType, stmt, c.getCharset(), c);
		} catch (Exception e) {
			StringBuilder s = new StringBuilder();
			logger.warn(s.append(c).append(stmt).toString()+" error:"+ e);
			String msg = e.getMessage();
			c.writeErrMessage(ErrorCode.ER_PARSE_ERROR, msg == null ? e
					.getClass().getSimpleName() : msg);
			return null;
		}
	}

    private static boolean isSqlEngineSeq(String stmt, SchemaConfig schema)
    {
        if(pattern.matcher(stmt).find()) {
			return true;
		}
        SQLStatementParser parser =new MySqlStatementParser(stmt);
        MySqlInsertStatement statement = (MySqlInsertStatement) parser.parseStatement();
        String tableName=   statement.getTableName().getSimpleName();
        TableConfig tableConfig= schema.getTables().get(tableName.toUpperCase());
        if(tableConfig==null) {
			return false;
		}
        if(tableConfig.isAutoIncrement())
        {
            boolean isHasIdInSql=false;
            String primaryKey = tableConfig.getPrimaryKey();
            List<SQLExpr> columns = statement.getColumns();
            for (SQLExpr column : columns)
            {
                String columnName = column.toString();
                if(primaryKey.equalsIgnoreCase(columnName))
                {
                    isHasIdInSql = true;
                    break;
                }
            }
            if(!isHasIdInSql) {
				return true;
			}
        }
        return false;
    }

}
