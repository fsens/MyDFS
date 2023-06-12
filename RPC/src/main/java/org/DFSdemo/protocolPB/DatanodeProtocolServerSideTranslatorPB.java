package org.DFSdemo.protocolPB;

import org.DFSdemo.protocol.*;

/**
 * DatanodeProtocol实现类与DatanodeProtocolPB实现类之间的适配器，在服务端上（CS模式的服务端）上
 */
public class DatanodeProtocolServerSideTranslatorPB implements DatanodeProtocol {
    public boolean registerDatanode(DatanodeInfo datanodeinfo) {
        return false;
    }

    public DatanodeCommand blockReport(DatanodeInfo datanodeinfo) {
        return null;
    }

    public DatanodeCommand cacheReport(DatanodeInfo datanodeinfo) {
        return null;
    }

    public HeartbeatResponse sendHeartbeat(DatanodeHeartInfo datanodeheartinfo) {
        return null;
    }

    public void reportBadBlocks(LocatedBlock[] blocks) {

    }

    public void blockReceivedAndDeleted(DatanodeInfo datanodeinfo, StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks) {

    }

    public void commitBlockSynchronization(DatanodeInfo[] datanodeinfos) {

    }

    //TODO:反序列化
}
