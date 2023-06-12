package org.DFSdemo.protocolPB;

import org.DFSdemo.protocol.ExtendedBlock;
import org.DFSdemo.protocol.InterDatanodeProtocol;
import org.DFSdemo.protocol.RecoveringBlock;
import org.DFSdemo.protocol.ReplicaRecoveryInfo;

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
