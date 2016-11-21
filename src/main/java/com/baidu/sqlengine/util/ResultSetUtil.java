package com.baidu.sqlengine.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.baidu.sqlengine.backend.mysql.protocol.FieldPacket;
import com.baidu.sqlengine.backend.mysql.protocol.RowDataPacket;
import com.baidu.sqlengine.constant.MysqlDefs;

public class ResultSetUtil {

    public static int toFlag(ResultSetMetaData metaData, int column)
            throws SQLException {

        int flags = 0;
        if (metaData.isNullable(column) == 1) {
            flags |= 1;
        }

        if (metaData.isSigned(column)) {
            flags |= 16;
        }

        if (metaData.isAutoIncrement(column)) {
            flags |= 128;
        }

        return flags;
    }

    public static void resultSetToFieldPacket(String charset, List<FieldPacket> fieldPks, ResultSet rs)
            throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();
        if (colunmCount > 0) {
            //String values="";
            for (int i = 0; i < colunmCount; i++) {
                int j = i + 1;
                FieldPacket fieldPacket = new FieldPacket();
                fieldPacket.orgName = StringUtil.encode(metaData.getColumnName(j), charset);
                fieldPacket.name = StringUtil.encode(metaData.getColumnLabel(j), charset);

                fieldPacket.orgTable = StringUtil.encode(metaData.getTableName(j), charset);
                fieldPacket.table = StringUtil.encode(metaData.getTableName(j), charset);
                fieldPacket.db = StringUtil.encode(metaData.getSchemaName(j), charset);
                fieldPacket.flags = toFlag(metaData, j);

                fieldPacket.length = metaData.getColumnDisplaySize(j);

                fieldPacket.decimals = (byte) metaData.getScale(j);
                int javaType = MysqlDefs.javaTypeDetect(
                        metaData.getColumnType(j), fieldPacket.decimals);
                fieldPacket.type = (byte) (MysqlDefs.javaTypeMysql(javaType) & 0xff);
                fieldPks.add(fieldPacket);
                //values+=metaData.getColumnLabel(j)+"|"+metaData.getColumnName(j)+"  ";
            }
            // System.out.println(values);
        }

    }

    public static RowDataPacket parseRowData(byte[] row,
                                             List<byte[]> fieldValues) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldValues.size());
        rowDataPkg.read(row);
        return rowDataPkg;
    }

    public static String getColumnValAsString(byte[] row,
                                              List<byte[]> fieldValues, int columnIndex) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldValues.size());
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(columnIndex);
        return new String(columnData);
    }

    public static byte[] getColumnVal(byte[] row, List<byte[]> fieldValues,
                                      int columnIndex) {
        RowDataPacket rowDataPkg = new RowDataPacket(fieldValues.size());
        rowDataPkg.read(row);
        byte[] columnData = rowDataPkg.fieldValues.get(columnIndex);
        return columnData;
    }

    public static byte[] fromHex(String hexString) {
        String[] hex = hexString.split(" ");
        byte[] b = new byte[hex.length];
        for (int i = 0; i < hex.length; i++) {
            b[i] = (byte) (Integer.parseInt(hex[i], 16) & 0xff);
        }

        return b;
    }

}
