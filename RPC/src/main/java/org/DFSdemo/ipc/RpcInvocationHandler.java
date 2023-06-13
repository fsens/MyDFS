package org.DFSdemo.ipc;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;

/**
 * 自定义动态代理的接口，继承了InvocationHandler接口和Closeable接口，后者能够释放该类持有的资源
 */
public interface RpcInvocationHandler extends InvocationHandler, Closeable {
}
