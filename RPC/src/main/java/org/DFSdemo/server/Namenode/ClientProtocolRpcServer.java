package org.DFSdemo.server.Namenode;

import com.google.protobuf.BlockingService;
import org.DFSdemo.conf.CommonConfigurationKeysPublic;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.ipc.ProtobufRpcEngine;
import org.DFSdemo.ipc.RPC;
import org.DFSdemo.protocol.ClientProtocol;
import org.DFSdemo.protocol.proto.ClientNamenodeProtocolProtos;
import org.DFSdemo.protocolPB.ClientNamenodeProtocolPB;
import org.DFSdemo.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 该类是ClientProtocol接口的实现类，处理所ClientProtocol的rpc调用
 * 它由{@link Namenode}创建、启动和停止
 *
 * 该类主要有两个功能
 * 1.实现Namenode对外提供的额接口
 * 2.创建Server对象并启动
 */
public class ClientProtocolRpcServer implements ClientProtocol {

    /** 日志 */
    private static final Log LOG = LogFactory.getLog(ClientProtocolRpcServer.class);

    /** 处理客户protobuf rpc调用的Server */
    protected final RPC.Server protoBufRpcServer;

    /**
     * ClientProtocolRpcServer的构造方法
     *
     * @param conf 配置
     * @throws IOException
     */
    public ClientProtocolRpcServer(Configuration conf) throws IOException{
        int handlerCount = conf.getInt(CommonConfigurationKeysPublic.NAMENODE_HANDLER_COUNT_KEY,
                CommonConfigurationKeysPublic.NAMENODE_HANDLER_COUNT_DEFAULT);

        RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class, ProtobufRpcEngine.class);

        /**
         * 创建接口对应的实例：ClientNamenodeProtocolPB.class--clientNamenodePbService
         *
         * 由于客户端传来方法名、参数等信息，如果是自己寻找服务端对应的方法，得自己编写反射，这样即麻烦，性能又低
         * 所以，利用protoBuf提供的newReflectiveBlockingService方法来避免自己编写反射调用
         */
        ClientNamenodeProtocolServerSideTranslatorPB
                clientNamenodeProtocolServerSideTranslatorPB = new ClientNamenodeProtocolServerSideTranslatorPB(this);
        BlockingService clientNamenodePbService = ClientNamenodeProtocolProtos.ClientNamenodeProtocol.newReflectiveBlockingService(clientNamenodeProtocolServerSideTranslatorPB);

        /** 获取服务端地址 */
        InetSocketAddress protoBufRpcServerAddr = Namenode.getProtoBufRpcServerAddress(conf);
        String bindHost= protoBufRpcServerAddr.getHostName();
        int bindPort = protoBufRpcServerAddr.getPort();
        LOG.info("RPC server is binding to" + bindHost + ":" + bindPort);
        /** 利用Build构造protoBufRpcServer对象 */
        this.protoBufRpcServer = new RPC.Builder(conf)
                .setProtocol(ClientNamenodeProtocolPB.class)
                .setInstance(clientNamenodePbService)
                .setBindAddress(bindHost)
                .setBindPort(bindPort)
                .setNumHandlers(handlerCount)
                .setVerbose(true)
                .build();
    }

    /**
     * 启动RPC服务端
     */
    void start(){
        protoBufRpcServer.start();
    }

    void join() throws InterruptedException{
        protoBufRpcServer.join();
    }

    @Override
    public boolean rename2(String src, String dst) throws IOException {
        //TODO:Namenode实现rename2
        return false;
    }
}
