package com.baidu.sqlengine.backend.mysql.nio;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import com.baidu.sqlengine.backend.datasource.PhysicalDatasource;
import com.baidu.sqlengine.backend.heartbeat.DBHeartbeat;
import com.baidu.sqlengine.backend.heartbeat.MySQLHeartbeat;
import com.baidu.sqlengine.util.SecurityUtil;
import com.baidu.sqlengine.backend.mysql.nio.handler.ResponseHandler;
import com.baidu.sqlengine.constant.Capabilities;
import com.baidu.sqlengine.config.model.DBHostConfig;
import com.baidu.sqlengine.config.model.DataHostConfig;
import com.baidu.sqlengine.backend.mysql.protocol.AuthPacket;
import com.baidu.sqlengine.backend.mysql.protocol.BinaryPacket;
import com.baidu.sqlengine.backend.mysql.protocol.EOFPacket;
import com.baidu.sqlengine.backend.mysql.protocol.ErrorPacket;
import com.baidu.sqlengine.backend.mysql.protocol.HandshakePacket;
import com.baidu.sqlengine.backend.mysql.protocol.OkPacket;
import com.baidu.sqlengine.backend.mysql.protocol.QuitPacket;
import com.baidu.sqlengine.backend.mysql.protocol.Reply323Packet;

public class MySQLDataSource extends PhysicalDatasource {

    private final MySQLConnectionFactory factory;

    public MySQLDataSource(DBHostConfig config, DataHostConfig hostConfig,
                           boolean isReadNode) {
        super(config, hostConfig, isReadNode);
        this.factory = new MySQLConnectionFactory();

    }

    @Override
    public void createNewConnection(ResponseHandler handler, String schema) throws IOException {
        factory.make(this, handler, schema);
    }

    private long getClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        // flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        // flag |= Capabilities.CLIENT_MULTI_RESULTS;        
        return flag;
    }

    private byte[] passwd(String pass, HandshakePacket hs) throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] passwd = pass.getBytes();
        int sl1 = hs.seed.length;
        int sl2 = hs.restOfScrambleBuff.length;
        byte[] seed = new byte[sl1 + sl2];
        System.arraycopy(hs.seed, 0, seed, 0, sl1);
        System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
        return SecurityUtil.scramble411(passwd, seed);
    }

    @Override
    public boolean testConnection(String schema) throws IOException {

        boolean isConnected = true;

        Socket socket = null;
        InputStream in = null;
        OutputStream out = null;
        try {
            socket = new Socket(this.getConfig().getIp(), this.getConfig().getPort());
            socket.setSoTimeout(1000 * 20);
            socket.setReceiveBufferSize(32768);
            socket.setSendBufferSize(32768);
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);

            in = new BufferedInputStream(socket.getInputStream(), 32768);
            out = new BufferedOutputStream(socket.getOutputStream(), 32768);

            /**
             * Phase 1: MySQL to client. Send handshake packet.
             */
            BinaryPacket bin1 = new BinaryPacket();
            bin1.read(in);

            HandshakePacket handshake = new HandshakePacket();
            handshake.read(bin1);

            /**
             * Phase 2: client to MySQL. Send auth packet.
             */
            AuthPacket authPacket = new AuthPacket();
            authPacket.packetId = 1;
            authPacket.clientFlags = getClientFlags();
            authPacket.maxPacketSize = 1024 * 1024 * 16;
            authPacket.charsetIndex = handshake.serverCharsetIndex & 0xff;
            authPacket.user = this.getConfig().getUser();
            ;
            try {
                authPacket.password = passwd(this.getConfig().getPassword(), handshake);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e.getMessage());
            }
            authPacket.database = schema;
            authPacket.write(out);
            out.flush();

            /**
             * Phase 3: MySQL to client. send OK/ERROR packet.
             */
            BinaryPacket bin2 = new BinaryPacket();
            bin2.read(in);
            switch (bin2.data[0]) {
                case OkPacket.FIELD_COUNT:
                    break;
                case ErrorPacket.FIELD_COUNT:
                    ErrorPacket err = new ErrorPacket();
                    err.read(bin2);
                    isConnected = false;
                case EOFPacket.FIELD_COUNT:
                    // 发送323响应认证数据包
                    Reply323Packet r323 = new Reply323Packet();
                    r323.packetId = ++bin2.packetId;
                    String passwd = this.getConfig().getPassword();
                    if (passwd != null && passwd.length() > 0) {
                        r323.seed = SecurityUtil.scramble323(passwd, new String(handshake.seed)).getBytes();
                    }
                    r323.write(out);
                    out.flush();
                    break;
            }

        } catch (IOException e) {
            isConnected = false;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
            }

            try {
                if (out != null) {
                    out.write(QuitPacket.QUIT);
                    out.flush();
                    out.close();
                }
            } catch (IOException e) {
            }

            try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e) {
            }
        }

        return isConnected;
    }

    @Override
    public DBHeartbeat createHeartBeat() {
        return new MySQLHeartbeat(this);
    }

}