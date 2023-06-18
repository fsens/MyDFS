package org.DFSdemo.util;

import com.google.protobuf.ByteString;
import org.DFSdemo.ipc.RPC;
import org.DFSdemo.ipc.protobuf.IpcConnectionContextProtos;
import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;

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


    //构造建立连接后发送的上下文的包装类的对象。具体方法请求的包装类对象在适配器那里就构造好了

    /**
     * 构造IpcConnectionContextProto对象
     *
     * @param protocolName 协议字段
     * @return 构造好的IpcConnectionContextProto对象
     */
    public static IpcConnectionContextProtos.IpcConnectionContextProto makeIpcConnectionContext(
            final String protocolName){
        IpcConnectionContextProtos.IpcConnectionContextProto.Builder retBuilder = IpcConnectionContextProtos.IpcConnectionContextProto.newBuilder();
        retBuilder.setProtocol(protocolName);

        return retBuilder.build();
    }

    static RpcHeaderProtos.RpcKindPtoto convertRpcKind(RPC.RpcKind rpcKind){
        switch (rpcKind){
            case RPC_BUILTIN : return RpcHeaderProtos.RpcKindPtoto.RPC_BUILDIN;
            case RPC_PROTOCOL_BUFFER: return RpcHeaderProtos.RpcKindPtoto.RPC_PROTOCOL_BUFFER;
            default: return null;
        }
    }

    public static RPC.RpcKind convertRpcKind(RpcHeaderProtos.RpcKindPtoto rpcKind){
        switch (rpcKind){
            case RPC_BUILDIN: return RPC.RpcKind.RPC_BUILTIN;
            case RPC_PROTOCOL_BUFFER: return RPC.RpcKind.RPC_PROTOCOL_BUFFER;
            default: return null;
        }
    }

    /**
     * 构造RpcRequestHeaderProto类型的对象
     *
     * @param rpcKind 序列化方式
     * @param operation 操作
     * @param callId 调用单元的id
     * @param clientId 客户端的id
     * @param retryCount 重连次数
     * @return 构造好的RpcRequestHeaderProto类型的对象
     */
    public static RpcHeaderProtos.RpcRequestHeaderProto makeRpcRequestHeader(RPC.RpcKind rpcKind,
                                                                             RpcHeaderProtos.RpcRequestHeaderProto.OperationProto operation,
                                                                             int callId,
                                                                             byte[] clientId,
                                                                             int retryCount){
        RpcHeaderProtos.RpcRequestHeaderProto.Builder retBuilder = RpcHeaderProtos.RpcRequestHeaderProto.newBuilder();

        retBuilder.setRpcKind(convertRpcKind(rpcKind))
                .setRpcOp(operation)
                .setCallId(callId)
                .setClientId(ByteString.copyFrom(clientId))
                .setRetryCount(retryCount);
        return retBuilder.build();
    }
}
