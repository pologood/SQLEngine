package com.baidu.sqlengine.route.sequence.handler;

public interface SequenceHandler {

    long nextId(String prefixName);

}