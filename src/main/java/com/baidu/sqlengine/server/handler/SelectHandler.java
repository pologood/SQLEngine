package com.baidu.sqlengine.server.handler;

import com.baidu.sqlengine.parser.util.ParseUtil;
import com.baidu.sqlengine.server.ServerConnection;
import com.baidu.sqlengine.server.parser.ServerParse;
import com.baidu.sqlengine.server.parser.ServerParseSelect;
import com.baidu.sqlengine.server.response.SelectDatabase;
import com.baidu.sqlengine.server.response.SelectIdentity;
import com.baidu.sqlengine.server.response.SelectLastInsertId;
import com.baidu.sqlengine.server.response.SelectTxReadOnly;
import com.baidu.sqlengine.server.response.SelectUser;
import com.baidu.sqlengine.server.response.SelectVariables;
import com.baidu.sqlengine.server.response.SelectVersion;
import com.baidu.sqlengine.server.response.SelectVersionComment;
import com.baidu.sqlengine.server.response.SessionIncrement;
import com.baidu.sqlengine.server.response.SessionIsolation;

public final class SelectHandler {

    public static void handle(String stmt, ServerConnection c, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(c);
                break;
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(c);
                break;
            case ServerParseSelect.USER:
                SelectUser.response(c);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(c);
                break;
            case ServerParseSelect.SESSION_INCREMENT:
                SessionIncrement.response(c);
                break;
            case ServerParseSelect.SESSION_ISOLATION:
                SessionIsolation.response(c);
                break;
            case ServerParseSelect.LAST_INSERT_ID:
                // offset = ParseUtil.move(stmt, 0, "select".length());
                loop:
                for (int l = stmt.length(); offset < l; ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case 'L':
                        case 'l':
                            break loop;
                    }
                }
                offset = ServerParseSelect.indexAfterLastInsertIdFunc(stmt, offset);
                offset = ServerParseSelect.skipAs(stmt, offset);
                SelectLastInsertId.response(c, stmt, offset);
                break;
            case ServerParseSelect.IDENTITY:
                // offset = ParseUtil.move(stmt, 0, "select".length());
                loop:
                for (int l = stmt.length(); offset < l; ++offset) {
                    switch (stmt.charAt(offset)) {
                        case ' ':
                            continue;
                        case '/':
                        case '#':
                            offset = ParseUtil.comment(stmt, offset);
                            continue;
                        case '@':
                            break loop;
                    }
                }
                int indexOfAtAt = offset;
                offset += 2;
                offset = ServerParseSelect.indexAfterIdentity(stmt, offset);
                String orgName = stmt.substring(indexOfAtAt, offset);
                offset = ServerParseSelect.skipAs(stmt, offset);
                SelectIdentity.response(c, stmt, offset, orgName);
                break;
            case ServerParseSelect.SELECT_VAR_ALL:
                SelectVariables.execute(c, stmt);
                break;
            case ServerParseSelect.SESSION_TX_READ_ONLY:
                SelectTxReadOnly.response(c);
                break;
            default:
                c.execute(stmt, ServerParse.SELECT);
        }
    }

}
