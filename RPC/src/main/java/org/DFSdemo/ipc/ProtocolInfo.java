package org.DFSdemo.ipc;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 用户端与服务端连接的协议
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ProtocolInfo {
    String protocolName();//协议名称
}
