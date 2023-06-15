//package org.DFSdemo.protocolTranslatorPB;
//
//import org.DFSdemo.protocol.DatanodeInfo;
//import org.DFSdemo.protocol.DatanodeProtocol;
//
//public class DatanodeProtocolTranslator implements DatanodeProtocol {
//    RPCproxy proxy;
//
//    public void DatanodeProtocolTranslator(RPCproxy proxy){
//        this.proxy=proxy;
//    }
//
//    public boolean registerDatanode(DatanodeInfo datanodeinfo){
//        RequestProto request = ProtoUtils.makeProtoRequest(DatanodeInfo datanodeinfo);
//
//        proxy.registerDatanode(request);
//    }
//
//
//}
