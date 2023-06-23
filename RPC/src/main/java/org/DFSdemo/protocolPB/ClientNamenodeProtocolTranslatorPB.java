package org.DFSdemo.protocolPB;

import com.google.protobuf.ServiceException;
import org.DFSdemo.ipc.RPC;
import org.DFSdemo.protocol.*;
import org.DFSdemo.protocol.proto.ClientNamenodeProtocolProtos.rename2RequestProto;

import java.io.Closeable;
import java.io.IOException;

/**
 * 由于要客户端那里需要代理的是ClientProtocol接口，而序列化后相应的接口是ClientNamenodeProtocolPB，所以需要一共适配器来来将两者进行适配
 */
public class ClientNamenodeProtocolTranslatorPB implements ClientProtocol, Closeable {
    private ClientNamenodeProtocolPB rpcProxy;

    public ClientNamenodeProtocolTranslatorPB(ClientNamenodeProtocolPB proxy) {
    }

    public void ClientNamenodeProtocolTranslatorPB(ClientNamenodeProtocolPB proxy){
        this.rpcProxy=proxy;
    }

//    public LocatedBlocks getBlockLocations(String src , long offset , long length){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src, long offset, long length);
//
//        proxy.getBlockLocations(request);
//    }
//
//    public HdfsFileStatus create(String src, String clientName, FsPermission masked){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName, FsPermission masked);
//
//        proxy.create(request);
//    }
//
//    public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes);
//
//        proxy.addBlock(request);
//    }
//
//    public boolean complete(String src, String clientName,ExtendedBlock last, long fileId){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName,ExtendedBlock last, long fileId);
//
//        proxy.complete(request);
//    }
//
//    public LocatedBlock append(String src, String clientName){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName);
//
//        proxy.append(request);
//    }

    @Override
    public boolean rename2(String src, String dst) throws IOException {
        rename2RequestProto request = rename2RequestProto.newBuilder()
                .setSrc(src)
                .setDst(dst)
                .build();

        try {
            /**
             * ClientNamenodeProtocolPB继承的ClientNamenodeProtocol.BlockingInterface将rename2方法的参数定义为了：
             *第一个为控制器，一般为null
             *第二个才是包装为了rename2RequestProto的请求
             */
            return rpcProxy.rename2(null, request).getResult();
        }catch (ServiceException e){
            throw new IOException();
        }
    }

//    public boolean mkdirs(String src, FsPermission masked){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src, FsPermission masked);
//
//        proxy.mkdirs(request);
//    }
//
//    public boolean delete(String src){
//        RequestProto request = ProtoUtils.makeProtoRequest(String src);
//
//        proxy.delete(request);
//    }

    @Override
    public void close() throws IOException{
        RPC.stopProxy(rpcProxy);
    }
}
