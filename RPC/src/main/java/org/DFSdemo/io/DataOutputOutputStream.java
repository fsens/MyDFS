package org.DFSdemo.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream的实现，包装类DataOutput
 *
 */
public class DataOutputOutputStream extends OutputStream {

    private final DataOutput out;

    private DataOutputOutputStream(DataOutput out){
        this.out = out;
    }

    /**
     * 从DataOutput对象中构造OutputStream对象
     *
     * @param out 需要适配的DataOut对象
     * @return 适配后的OutputStream对象
     */
    public static OutputStream constructDataOutputStream(DataOutput out){
        if (out instanceof OutputStream){
            return (OutputStream) out;
        }
        return new DataOutputOutputStream(out);
    }

    @Override
    public void write(int b) throws IOException{
        out.writeByte(b);
    }

    @Override
    public void write(byte[] b) throws IOException{
        out.write(b);
    }

    @Override
    public void write(byte[] b,int off, int  len) throws IOException{
        out.write(b, off, len);
    }
}
