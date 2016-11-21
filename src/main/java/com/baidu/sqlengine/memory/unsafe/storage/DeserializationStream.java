package com.baidu.sqlengine.memory.unsafe.storage;

public abstract class DeserializationStream {

    /** The most general-purpose method to read an object. */
    public abstract <T> T readObject();
    /** Reads the object representing the key of a key-value pair. */
    public <T> T readKey(){return readObject();}
    /** Reads the object representing the value of a key-value pair. */
    public <T> T readValue(){ return readObject();}
    public abstract void close();
}
