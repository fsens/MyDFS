package org.DFSdemo.ipc;

import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;

import java.io.IOException;

public class RpcServerException extends IOException {
    public RpcServerException(String message){
        super(message);
    }

    public RpcServerException(String message, Throwable cause){
        super(message, cause);
    }

    public RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto getRpcStatusProto(){
        return RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR;
    }

    public RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto getRpcErrorCodeProto(){
        return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_RPC_SERVER;
    }
}
