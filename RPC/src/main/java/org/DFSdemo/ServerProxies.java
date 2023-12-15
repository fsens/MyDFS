package org.DFSdemo;

import org.DFSdemo.conf.Configuration;
import org.DFSdemo.ipc.ProtobufRpcEngine;
import org.DFSdemo.ipc.RPC;
import org.DFSdemo.protocol.ClientProtocol;
import org.DFSdemo.protocolPB.ClientNamenodeProtocolPB;
import org.DFSdemo.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.DFSdemo.server.Namenode.Namenode;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

/**
 * 这是一个代理工具类，用来实现获取各个协议代理的逻辑
 */
public class ServerProxies {

    /** ProxyInfo封装代理对象，它相当于一个盛装代理对象的容器。除了代理对象外，还可以有其它相关信息 */
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

    /**
     * 创建代理对象，不同的协议代理获取被集成到了该方法中
     * @param conf 配置对象
     * @param uri 服务端地址
     * @param xface 代理对象的接口
     * @return 代理对象
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public static <T> ProxyInfo<T> createProxy(Configuration conf, URI uri,Class<T> xface) throws IOException{
        InetSocketAddress address = Namenode.getAddress(uri);

        T proxy;
        if (xface == ClientProtocol.class){
            proxy = (T) createNamenodeProxyWithClientProtocol(conf, address);
        }
        else {
            String message = "Unsupported protocol found when creating the proxy " + "connection to NameNode: " + ((xface!=null) ? xface.getName():"null");
            throw new IllegalStateException(message);
        }
        return new ProxyInfo<>(proxy, address);
    }

    /**
     * 创建ClientProtocol的代理对象
     * @param conf 配置对象
     * @param address 服务器地址
     * @return ClientProtocol的代理对象
     * @throws IOException
     */
    private static ClientProtocol createNamenodeProxyWithClientProtocol(Configuration conf,InetSocketAddress address) throws IOException{
        //目前还不支持同一个ClientNamenodeProtocolPB（协议接口）对应多个序列化引擎
        RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);

        int rpcTimeOut = 6000;//设置超时阈值为6s
        /** 获取ClientNamenodeProtocolPB的代理类 */
        ClientNamenodeProtocolPB proxy = RPC.getProtocolProxy(ClientNamenodeProtocolPB.class, address, conf, SocketFactory.getDefault(), rpcTimeOut);
        return new ClientNamenodeProtocolTranslatorPB(proxy);
    }
}

