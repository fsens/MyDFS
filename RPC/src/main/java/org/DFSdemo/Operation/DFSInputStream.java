package org.DFSdemo.Operation;

/**
 * 数据输入流，定义在Protocol接口之上的类
 */
public class DFSInputStream {
    //TODO:构造方法会根据传入的参数调用getBlockLocations()方法
    // 调用DataTransferProtocol.readBlock(),建立与对应Datanode的TCP连接,然后读取对应的块
    // 该类中还定义了读取器reader,给Client提供读取的API
    // 读取完毕后，会有通知相应方法关闭输入流的方法
}
