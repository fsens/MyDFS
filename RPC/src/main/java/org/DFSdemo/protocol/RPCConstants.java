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

    /** 定义重试次数为-1，即不启用重试机制 */

    public static final int INVALID_RETRY_COUNT = -1;

    /** 在特殊情况下使用的ClientId，如没有真实客户端的情况下 */
    public static final byte[] DUMMY_CLIENT_ID = new byte[0];

    /** 定义发送连接上下文的callId为-3 */
    public final static int CONNECTION_CONTEXT_CALL_ID = -3;

}
