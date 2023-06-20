package org.DFSdemo.ipc;

import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;

import java.io.IOException;

public class RemoteException extends IOException {

    private final int errorCode;
    private String className;

    RemoteException(String className, String msg){
        super(msg);
        this.className = className;
        errorCode = -1;
    }

    RemoteException(String className, String msg, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errCode){
        super(msg);
        this.className = className;

        if (errCode == null){
            errorCode = -1;
        }else {
            errorCode = errCode.getNumber();
        }
    }

    @Override
    public String toString(){
        return getClass().getName() + "(" + className + ")" + getMessage();
    }
}
