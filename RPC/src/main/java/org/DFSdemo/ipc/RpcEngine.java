package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 实现RPC的接口
 * 定义该接口还可以方便扩展序列化引擎
 */
public interface RpcEngine {

    /**
     * 获取客户端的代理对象
     *
     * @param protocol 需要代理的接口
     * @param address 服务端地址
     * @param conf 配置
     * @param factory 创建socket的工厂
     * @param rpcTimeout rpc超时时间
     * @return 接口的代理对象
     * @param <T> 表明该方法是个与类无关的泛型方法
     * @throws IOException
     */
    <T> T getProxy(Class<T> protocol,
                   InetSocketAddress address,
                   Configuration conf,
                   SocketFactory factory,
                   int rpcTimeout) throws IOException;

    /**
     * 返回一个Server实例
     *
     * @param protocol 接口（协议）
     * @param instance 接口（协议）的实例
     * @param bindAddress 服务端地址
     * @param port 服务端接口
     * @param numHandlers Handler线程的数量
     * @param numReaders Reader线程的数量
     * @param queueSizePerHandler 每个Handler期望的消息队列大小
     * @param verbose 是否对调用信息打log
     * @param conf Configuration对象
     * @return Server实例
     * @throws IOException
     */
    RPC.Server getServer(Class<?> protocol, Object instance,
                         String bindAddress, int port,
                         int numHandlers, int numReaders,
                         int queueSizePerHandler, boolean verbose, Configuration conf) throws IOException;
}
