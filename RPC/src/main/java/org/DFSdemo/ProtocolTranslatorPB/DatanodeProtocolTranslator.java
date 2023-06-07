package org.DFSdemo.ProtocolTranslatorPB;

import org.DFSdemo.Protocol.DatanodeInfo;
import org.DFSdemo.Protocol.DatanodeProtocol;

public class DatanodeProtocolTranslator implements DatanodeProtocol {
    RPCproxy proxy;

    public void DatanodeProtocolTranslator(RPCproxy proxy){
        this.proxy=proxy;
    }

    public boolean registerDatanode(DatanodeInfo datanodeinfo){
        RequestProto request = ProtoUtils.makeProtoRequest(DatanodeInfo datanodeinfo);

        proxy.registerDatanode(request);
    }


}
