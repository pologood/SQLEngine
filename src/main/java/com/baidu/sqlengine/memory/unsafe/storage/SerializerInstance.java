package com.baidu.sqlengine.memory.unsafe.storage;

import java.io.InputStream;
import java.io.OutputStream;

public abstract class SerializerInstance {

    protected abstract SerializationStream serializeStream(OutputStream s);

    protected abstract DeserializationStream deserializeStream(InputStream s);
}
