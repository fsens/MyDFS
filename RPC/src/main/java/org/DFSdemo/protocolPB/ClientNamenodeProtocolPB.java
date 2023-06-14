package org.DFSdemo.protocolPB;

import org.DFSdemo.ipc.ProtocolInfo;
import org.DFSdemo.protocol.RPCConstants;
import org.DFSdemo.protocol.proto.ClientNamenodeProtocolProtos.ClientNamenodeProtocol;

/**
 * Client和Namenode之间的PB层接口
 */

@ProtocolInfo(protocolName = RPCConstants.CLIENT_NAMENODE_PROTOCOL_NAME)
public interface ClientNamenodeProtocolPB extends ClientNamenodeProtocol.BlockingInterface{
    //TODO:ClientProtocol接口的定义的PB形式
}
