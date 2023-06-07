package org.DFSdemo.Protocol;

/**
 * Datanode之间进行通信的协议
 */
public interface InterDatanodeProtocol {
    /**
     * 初始化租约恢复
     *
     * @param rBlock 主恢复节点的信息，其中包含了Namenode下发的新的时间戳
     * @return 封装了数据流管道中所有Datanode上指定的数据块状态信息,主恢复节点根据这些数据块的信息来确定新的数据长度
     */
    ReplicaRecoveryInfo initReplicaRecovery(RecoveringBlock rBlock);

    /**
     * 将数据流管道中相关的数据块长度更新为新的长度,将时间戳更新为新的时间戳
     *
     * @param oldBlock 对应的数据块
     * @param recoveryId 新的时间戳
     * @param newLength 新的数据长度
     */
    void updateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId, long newLength);
}
