package com.baidu.sqlengine.config;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import com.baidu.sqlengine.SqlEngineServer;
import com.baidu.sqlengine.config.model.FirewallConfig;
import com.baidu.sqlengine.config.model.UserConfig;
import com.baidu.sqlengine.config.model.UserPrivilegesConfig;
import com.baidu.sqlengine.constant.Alarms;
import com.baidu.sqlengine.net.handler.FrontendPrivileges;
import com.baidu.sqlengine.parser.druid.SqlEngineSchemaStatVisitor;
import com.baidu.sqlengine.parser.druid.SqlEngineStatementParser;

public class SqlEnginePrivileges implements FrontendPrivileges {
    /**
     * 无需每次建立连接都new实例。
     */
    private static SqlEnginePrivileges instance = new SqlEnginePrivileges();

    private static final Logger ALARM = LoggerFactory.getLogger("alarm");

    private static boolean check = false;
    private final static ThreadLocal<WallProvider> contextLocal = new ThreadLocal<WallProvider>();

    public static SqlEnginePrivileges instance() {
        return instance;
    }

    private SqlEnginePrivileges() {
        super();
    }

    @Override
    public boolean schemaExists(String schema) {
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        return conf.getSchemas().containsKey(schema);
    }

    @Override
    public boolean userExists(String user, String host) {
        //检查用户及白名单
        return checkFirewallWhiteHostPolicy(user, host);
    }

    @Override
    public String getPassword(String user) {
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        if (user != null && user.equals(conf.getSystem().getClusterHeartbeatUser())) {
            return conf.getSystem().getClusterHeartbeatPass();
        } else {
            UserConfig uc = conf.getUsers().get(user);
            if (uc != null) {
                return uc.getPassword();
            } else {
                return null;
            }
        }
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getSchemas();
        } else {
            return null;
        }
    }

    @Override
    public Boolean isReadOnly(String user) {
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.isReadOnly();
        } else {
            return null;
        }
    }

    @Override
    public int getBenchmark(String user) {
        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        UserConfig uc = conf.getUsers().get(user);
        if (uc != null) {
            return uc.getBenchmark();
        } else {
            return 0;
        }
    }

    @Override
    public boolean checkFirewallWhiteHostPolicy(String user, String host) {

        SqlEngineConfig sqlEngineConfig = SqlEngineServer.getInstance().getConfig();
        FirewallConfig firewallConfig = sqlEngineConfig.getFirewall();

        //防火墙 白名单处理
        boolean isPassed = false;

        Map<String, List<UserConfig>> whitehost = firewallConfig.getWhitehost();
        if (whitehost == null || whitehost.size() == 0) {
            Map<String, UserConfig> users = sqlEngineConfig.getUsers();
            isPassed = users.containsKey(user);

        } else {
            List<UserConfig> list = whitehost.get(host);
            if (list != null) {
                for (UserConfig userConfig : list) {
                    if (userConfig.getName().equals(user)) {
                        isPassed = true;
                        break;
                    }
                }
            }
        }

        if (!isPassed) {
            ALARM.error(new StringBuilder().append(Alarms.FIREWALL_ATTACK).append("[host=").append(host)
                    .append(",user=").append(user).append(']').toString());
            return false;
        }
        return true;
    }

    /**
     * @see https://github.com/alibaba/druid/wiki/%E9%85%8D%E7%BD%AE-wallfilter
     */
    @Override
    public boolean checkFirewallSQLPolicy(String user, String sql) {

        boolean isPassed = true;

        if (contextLocal.get() == null) {
            FirewallConfig firewallConfig = SqlEngineServer.getInstance().getConfig().getFirewall();
            if (firewallConfig != null) {
                if (firewallConfig.isCheck()) {
                    contextLocal.set(firewallConfig.getProvider());
                    check = true;
                }
            }
        }

        if (check) {
            WallCheckResult result = contextLocal.get().check(sql);

            // 修复 druid 防火墙在处理SHOW FULL TABLES WHERE Table_type != 'VIEW' 的时候存在的 BUG
            List<SQLStatement> stmts = result.getStatementList();
            if (!stmts.isEmpty() && !(stmts.get(0) instanceof SQLShowTablesStatement)) {
                if (!result.getViolations().isEmpty()) {
                    isPassed = false;
                    ALARM.warn("Firewall to intercept the '" + user + "' unsafe SQL , errMsg:"
                            + result.getViolations().get(0).getMessage() +
                            " \r\n " + sql);
                }
            }

        }
        return isPassed;
    }

    // 审计SQL权限
    @Override
    public boolean checkDmlPrivilege(String user, String schema, String sql) {

        if (schema == null) {
            return true;
        }

        boolean isPassed = false;

        SqlEngineConfig conf = SqlEngineServer.getInstance().getConfig();
        UserConfig userConfig = conf.getUsers().get(user);
        if (userConfig != null) {

            UserPrivilegesConfig userPrivilege = userConfig.getPrivilegesConfig();
            if (userPrivilege != null && userPrivilege.isCheck()) {

                UserPrivilegesConfig.SchemaPrivilege schemaPrivilege = userPrivilege.getSchemaPrivilege(schema);
                if (schemaPrivilege != null) {

                    String tableName = null;
                    int index = -1;

                    //TODO 此处待优化，寻找更优SQL 解析器
                    SQLStatementParser parser = new SqlEngineStatementParser(sql);
                    SQLStatement stmt = parser.parseStatement();

                    if (stmt instanceof MySqlReplaceStatement || stmt instanceof SQLInsertStatement) {
                        index = 0;
                    } else if (stmt instanceof SQLUpdateStatement) {
                        index = 1;
                    } else if (stmt instanceof SQLSelectStatement) {
                        index = 2;
                    } else if (stmt instanceof SQLDeleteStatement) {
                        index = 3;
                    }

                    if (index > -1) {

                        SchemaStatVisitor schemaStatVisitor = new SqlEngineSchemaStatVisitor();
                        stmt.accept(schemaStatVisitor);
                        String key = schemaStatVisitor.getCurrentTable();
                        if (key != null) {

                            if (key.contains("`")) {
                                key = key.replaceAll("`", "");
                            }

                            int dotIndex = key.indexOf(".");
                            if (dotIndex > 0) {
                                tableName = key.substring(dotIndex + 1);
                            } else {
                                tableName = key;
                            }

                            //获取table 权限, 此处不需要检测空值, 无设置则自动继承父级权限
                            UserPrivilegesConfig.TablePrivilege tablePrivilege =
                                    schemaPrivilege.getTablePrivilege(tableName);
                            if (tablePrivilege.getDml()[index] > 0) {
                                isPassed = true;
                            }

                        } else {
                            //skip
                            isPassed = true;
                        }

                    } else {
                        //skip
                        isPassed = true;
                    }

                } else {
                    //skip
                    isPassed = true;
                }

            } else {
                //skip
                isPassed = true;
            }

        } else {
            //skip
            isPassed = true;
        }

        if (!isPassed) {
            ALARM.error(new StringBuilder().append(Alarms.DML_ATTACK).append("[sql=").append(sql)
                    .append(",user=").append(user).append(']').toString());
        }

        return isPassed;
    }

}