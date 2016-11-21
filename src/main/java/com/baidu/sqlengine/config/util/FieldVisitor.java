package com.baidu.sqlengine.config.util;

public interface FieldVisitor {

    void visit(String name, Class<?> type, Class<?> definedIn, Object value);

}