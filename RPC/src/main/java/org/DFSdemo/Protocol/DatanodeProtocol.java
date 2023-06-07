package org.DFSdemo.Protocol;

/**
 * Datanodes和Namenode之间的通信协议
 */
public interface DatanodeProtocol {
    /**
     * 向Namenode注册该数据节点
     *
     * @param datanodeinfo 该数据节点信息
     * @return 是否注册成功
     */
    public boolean registerDatanode(DatanodeInfo datanodeinfo);


    /**
     * 向Namenode上报自己存储数据的的信息
     *
     * @param datanodeinfo 封装了数据节点的所有信息
     * @return 对数据节点的响应，封装了对数据节点的一些指令
     */
    public DatanodeCommand blockReport(DatanodeInfo datanodeinfo);


    /**
     * 向Namenode上报自己缓存的数据的信息
     *
     * @param datanodeinfo 封装了数据节点的所有信息
     * @return 对数据节点的响应，封装了对数据节点的一些指令
     */
    public DatanodeCommand cacheReport(DatanodeInfo datanodeinfo);

    /**
     * 发送心跳
     *
     * @param datanodeheartinfo 封装的心跳信息，默认3s发一次
     * @return Namenode对发送来心跳的数据节点的响应
     */
    public HeartbeatResponse sendHeartbeat(DatanodeHeartInfo datanodeheartinfo);

    /**
     * 向Namenode发送损坏的数据块的信息:DataBlockScanner线程会定期校验datanode上的校验码，
     * 会通过reportBadBlocks()将错误的数据块的元数据传给Namenode，以便Namenode对元数据进行更改
     *
     * @param blocks 损坏的数据块的信息
     */
    public void reportBadBlocks(LocatedBlock[] blocks);

    /**
     * 向Namenode汇报新增或者删除的数据块
     *
     * @param datanodeinfo 数据节点的信息
     * @param rcvdAndDeletedBlocks 新增或者删除了的数据块信息
     */
    public void blockReceivedAndDeleted(DatanodeInfo datanodeinfo,StorageReceivedDeletedBlocks[] rcvdAndDeletedBlocks);

    /**
     * 完成租约恢复后，同步Namenode上该数据块的时间戳和数据块长度，保持Namenode和数据节点的一致。
     *
     * @param datanodeinfos 租约恢复操作涉及的数据节点的信息
     */
    public void commitBlockSynchronization(DatanodeInfo[] datanodeinfos);
}
