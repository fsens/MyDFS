package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

/**
 *实现RpcEngine接口，该类为客户端接口提供代理
 */
public class ProtobufRpcEngine implements RpcEngine{

    /** 获取代理对象 */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol,
                          InetSocketAddress address,
                          Configuration conf,
                          SocketFactory factory,
                          int rpcTimeout) throws IOException {
        final Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeout);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(),new Class[]{protocol},invoker);
    }

    /** Invoker作为代理类，代理对象的方法实际上是在这里面定义的 */
    private static class Invoker implements RpcInvocationHandler {
        private Invoker(Class<?> protocol,
                        InetSocketAddress address,
                        Configuration configuration,
                        SocketFactory factory,
                        int rpcTimeOut){

        }

        /** 代理对象的方法定义处 */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
