package org.DFSdemo.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Writable {
    /**
     * 将对象的属性序列化后写入out
     *
     * @param out 数据输出流
     * @throws IOException
     */
    void write(DataOutput out) throws IOException;


    /**
     * 从in中反序列化对象的属性
     *
     * @param in 数据输入流
     * @throws IOException
     */
    void readFields(DataInput in) throws IOException;
}
