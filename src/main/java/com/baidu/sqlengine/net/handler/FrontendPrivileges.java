
package com.baidu.sqlengine.net.handler;

import java.util.Set;

/**
 * 权限提供者
 */
public interface FrontendPrivileges {

    /**
     * 检查schema是否存在
     */
    boolean schemaExists(String schema);

    /**
     * 检查用户是否存在，并且可以使用host实行隔离策略。
     */
    boolean userExists(String user, String host);

    /**
     * 提供用户的服务器端密码
     */
    String getPassword(String user);

    /**
     * 提供有效的用户schema集合
     */
    Set<String> getUserSchemas(String user);

    /**
     * 检查用户是否为只读权限
     *
     * @param user
     *
     * @return
     */
    Boolean isReadOnly(String user);

    /**
     * 获取设定的系统最大连接数的降级阀值
     *
     * @param user
     *
     * @return
     */
    int getBenchmark(String user);

    /**
     * 检查防火墙策略
     * （白名单策略）
     *
     * @param user
     * @param host
     *
     * @return
     */
    boolean checkFirewallWhiteHostPolicy(String user, String host);

    /**
     * 检查防火墙策略
     * (SQL黑名单及注入策略)
     *
     * @param sql
     *
     * @return
     */
    boolean checkFirewallSQLPolicy(String user, String sql);

    /**
     * 检查 SQL 语句的 DML 权限
     *
     * @return
     */
    boolean checkDmlPrivilege(String user, String schema, String sql);

}