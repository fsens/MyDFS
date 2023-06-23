package org.DFSdemo.ipc;

import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;

public class RpcNoSuchMethodException extends RpcServerException{
    public RpcNoSuchMethodException(final String message){
        super(message);
    }

    @Override
    public RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto getRpcStatusProto(){
        return RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR;
    }

    @Override
    public RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto getRpcErrorCodeProto(){
        return RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_NO_SUCH_METHOD;
    }
}
