package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.Writable;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Client {
    /**
     * @param valueClass 调用的返回类型
     * @param conf 配置对象
     * @param factory socket工厂
     */
    public Client(Class<? extends Writable> valueClass, Configuration conf, SocketFactory factory){

    }

    public void stop(){

    }

    /**
     * 该类用来存储与连接相关的address、protocol等信息，标识网络连接
     */
    public static class ConnectionID{
        /**
         * 构造方法，定义了一些属性来标识连接
         *
         * @param address 服务端地址
         * @param protocol 协议
         * @param rpcTimeOut 超时时间
         * @param conf 配置
         */
        public ConnectionID(InetSocketAddress address,
                            Class<?> protocol,
                            int rpcTimeOut,
                            Configuration conf){

        }
    }

    /**
     * 调用RPC服务端，它相关信息定义在了remoteId中
     *
     * 这个方法是暴露给上游调用的
     *
     * @param rpcKind rpc类型（序列化/反序列化方式）。包含这个参数可以让相同rpc的传输用相同的套接字连接，不会和其它的rpc传输混淆
     * @param rpcRequest 包含序列化方法和参数
     * @param remoteId rpc server的信息
     * @return rpc返回值
     * @throws IOException
     */
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionID remoteId) throws IOException{
        return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT);
    }

    /**
     * 调用rpc服务端，相关信息定义在了remoteId中
     *
     * @param rpcKind rpc类型
     * @param rpcRequest 客户端请求，包含序列化方法和参数等信息
     * @param remoteId rpc server
     * @param serviceClass rpc的服务类
     * @return rpc返回值
     * @throws IOException 抛网络异常或者远程代码执行异常
     */
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionID remoteId, int serviceClass) throws IOException{
        return null;
    }


}
