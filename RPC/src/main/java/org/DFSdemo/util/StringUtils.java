package org.DFSdemo.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StringUtils {
    /**
     * 将byte数组转成16进制字符串
     *
     * @param bytes 源数组
     * @param start 起始位置
     * @param end 截止位置
     * @return 16禁止字符串
     */
    public static String byteToHexString(byte[] bytes, int start, int end){
        if (bytes == null){
            throw new IllegalArgumentException("byte = null");
        }
        StringBuilder s = new StringBuilder();
        for (int i = start; i < end ; i++){
            //将每个字节转为2位的十六进制数
            s.append(String.format("%02x", bytes[i]));
        }
        return s.toString();
    }

    public static String byteToHexString(byte[] bytes){
        return byteToHexString(bytes, 0, bytes.length);
    }

    /**
     * 将异常堆栈信息转为字符串
     *
     * @param e 异常对象
     * @return 字符串化的异常堆栈信息
     */
    public static String stringifyException(Throwable e){
        StringWriter sw = new StringWriter();
        PrintWriter printWriter = new PrintWriter(sw);
        e.printStackTrace(printWriter);
        printWriter.close();
        return sw.toString();
    }
}
