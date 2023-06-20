package org.DFSdemo.ipc;

import java.io.IOException;

public class RpcClientException extends RpcException {

    RpcClientException(final String message){
        super(message);
    }

    RpcClientException(final String message, final Throwable cause){
        super(message, cause);
    }
}
