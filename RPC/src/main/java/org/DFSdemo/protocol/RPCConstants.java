package org.DFSdemo.protocol;

import java.nio.ByteBuffer;

/**
 * 该类定义一些与RPC协议相关的常量
 */
public class RPCConstants {

    /** 客户端和名字节点的协议 */
    public static final String CLIENT_NAMENODE_PROTOCOL_NAME = "org.DFSdemo.protocol.ClientProtocol";

    /** RPC连接发送header的头信息,cnrpc(client namenode rpc) */
    public static final ByteBuffer HEADER = ByteBuffer.wrap("cnrpc".getBytes());


    public static final int INVALID_RETRY_COUNT = -1;

    public static final byte[] DUMMY_CLIENT_ID = new byte[0];
}
