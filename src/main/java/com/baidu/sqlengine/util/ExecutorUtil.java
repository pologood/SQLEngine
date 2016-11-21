package com.baidu.sqlengine.util;

import java.util.concurrent.LinkedTransferQueue;

import com.baidu.sqlengine.util.tool.NameableExecutor;
import com.baidu.sqlengine.util.tool.NameableThreadFactory;

public class ExecutorUtil {

    public static final NameableExecutor create(String name, int size) {
        return create(name, size, true);
    }

    private static final NameableExecutor create(String name, int size, boolean isDaemon) {
        NameableThreadFactory factory = new NameableThreadFactory(name, isDaemon);
        return new NameableExecutor(name, size, new LinkedTransferQueue<Runnable>(), factory);
    }

}