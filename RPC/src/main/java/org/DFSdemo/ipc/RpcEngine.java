package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 实现RPC的接口
 */
public interface RpcEngine {

    /**
     * 获取客户端的代理对象
     * @param protocol 需要代理的接口
     * @param address 服务端地址
     * @param conf 配置
     * @param factory 创建socket的工厂
     * @param rpcTimeout rpc超时时间
     * @param <T> 表明该方法是个与类无关的泛型方法
     * @return 接口的代理对象
     * @param <T>
     * @throws IOException
     */
    <T> T getProxy(Class<T> protocol,
                   InetSocketAddress address,
                   Configuration conf,
                   SocketFactory factory,
                   int rpcTimeout) throws IOException;
}
