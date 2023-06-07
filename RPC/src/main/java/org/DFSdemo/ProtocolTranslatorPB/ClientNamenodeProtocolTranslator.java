package org.DFSdemo.ProtocolTranslatorPB;

import org.DFSdemo.Protocol.*;

public class ClientNamenodeProtocolTranslator implements ClientProtocol {
    RPCproxy proxy;

    public void ClientNamenodeProtocolTranslator(RPCproxy proxy){
        this.proxy=proxy;
    }

    public LocatedBlocks getBlockLocations(String src , long offset , long length){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, long offset, long length);

        proxy.getBlockLocations(request);
    }

    public HdfsFileStatus create(String src, String clientName, FsPermission masked){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName, FsPermission masked);

        proxy.create(request);
    }

    public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes);

        proxy.addBlock(request);
    }

    public boolean complete(String src, String clientName,ExtendedBlock last, long fileId){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName,ExtendedBlock last, long fileId);

        proxy.complete(request);
    }

    public LocatedBlock append(String src, String clientName){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, String clientName);

        proxy.append(request);
    }

    public void rename2(String src, String dst){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, String dst);

        proxy.rename2(request);
    }

    public boolean mkdirs(String src, FsPermission masked){
        RequestProto request = ProtoUtils.makeProtoRequest(String src, FsPermission masked);

        proxy.mkdirs(request);
    }

    public boolean delete(String src){
        RequestProto request = ProtoUtils.makeProtoRequest(String src);

        proxy.delete(request);
    }
}
