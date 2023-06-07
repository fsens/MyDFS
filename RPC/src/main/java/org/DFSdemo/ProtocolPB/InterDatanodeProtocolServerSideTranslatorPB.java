package org.DFSdemo.ProtocolPB;

import org.DFSdemo.Protocol.ExtendedBlock;
import org.DFSdemo.Protocol.InterDatanodeProtocol;
import org.DFSdemo.Protocol.RecoveringBlock;
import org.DFSdemo.Protocol.ReplicaRecoveryInfo;

/**
 * InterDatanodeProtocol接口实现类和InterDatanodeProtocolPB实现类之间的适配器，在服务端上（CS模式的服务端）
 */
public class InterDatanodeProtocolServerSideTranslatorPB implements InterDatanodeProtocol {
    public ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock) {
        return null;
    }

    public void updateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId, long newLength) {

    }

    //TODO:反序列化
}
