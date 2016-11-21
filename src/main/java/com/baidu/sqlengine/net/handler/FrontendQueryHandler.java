package com.baidu.sqlengine.net.handler;

/**
 * 查询处理器
 */
public interface FrontendQueryHandler {

    void query(String sql);

    void setReadOnly(Boolean readOnly);
}