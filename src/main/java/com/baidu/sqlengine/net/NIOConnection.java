
package com.baidu.sqlengine.net;

import java.io.IOException;
import java.nio.ByteBuffer;


public interface NIOConnection extends ClosableConnection{

    /**
     * connected 
     */
    void register() throws IOException;

    /**
     * 处理数据
     */
    void handle(byte[] data);

    /**
     * 写出一块缓存数据
     */
    void write(ByteBuffer buffer);
    
     
     
}