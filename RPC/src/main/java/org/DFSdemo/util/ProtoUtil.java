package org.DFSdemo.util;

import java.io.DataInput;
import java.io.IOException;

public class ProtoUtil {
    /**
     * 从输入流中读取变长的int类型变量
     * Protocol Buffer writeDelimitedTo 写入的长度使用该编码模式
     * Protocol Buffer的varInt编码使用的是小端模式
     *
     * @param in 输入流
     * @return 对边长类型varInt32的解码结果
     * @throws IOException 编码格式错误或者EOF
     */
    public static int readRawVarInt32(DataInput in) throws IOException{
        byte tmp = in.readByte();
        if (tmp >= 0){
            return tmp;
        }
        int result = tmp & 0x7f;//取后七位
        if ((tmp = in.readByte()) >= 0){
            result |= tmp << 7;//由于是小端模式，所以解码为int时将先读出来的字节放在后面
        }else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = in.readByte()) >= 0){
                result |= tmp << 14;
            }else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = in.readByte()) >= 0){
                    result |= tmp <<21;
                }else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = in.readByte()) <<28;
                    if (tmp < 0 ){
                        for (int i = 0;i < 5;i++){
                            if (in.readByte() >= 0 ){
                                return result;
                            }
                        }
                        throw new IOException("Malformed varint");
                    }
                }
            }
        }
        return result;
    }
}
