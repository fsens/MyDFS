package org.DFSdemo;

import org.DFSdemo.conf.Configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

/**
 * 这是一个代理工具类，用来实现获取代理的逻辑
 */
public class ServerProxies {

    /** ProxyInfo封装代理对象 */
    public static class ProxyInfo<PROXYTYPE>{
        private final PROXYTYPE proxy;
        private final InetSocketAddress address;

        public ProxyInfo(PROXYTYPE proxy, InetSocketAddress address) {
            this.proxy = proxy;
            this.address = address;
        }

        public PROXYTYPE getProxy() {
            return proxy;
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    /** 创建代理对象 */
    @SuppressWarnings("unchecked")
    public static <T> ProxyInfo<T> createProxy(Configuration conf, URI uri,Class<T> xface) throws IOException{
        return null;
    }
}
