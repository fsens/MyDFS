package org.DFSdemo.protocol;

/**
 * Client和Datanodes以及Datanode之间的数据传输协议(最底层，TCP协议)
 */
public interface DataTransferProtocol {
    /**
     * 读取对应数据块的数据
     * @param blk 要读取的数据块
     * @param clientName 读取数据块的用户名
     * @param blockOffset 读取数据块的起始位置
     * @param length 读取数据块数据的长度
     */
    public void readBlock(final ExtendedBlock blk,
                          final String clientName,
                          final long blockOffset,
                          final long length);

    /**
     * 写入数据进入目标数据块
     * @param blk 要写入的数据块
     * @param clientName 写入数据块的用户名
     * @param source 源数据块所在数据节点的信息（负载均衡时会用到）
     */
    public void writeBlock(final ExtendedBlock blk,
                           final String clientName,
                           final DatanodeInfo source);


    /**
     * 数据转移(负载均衡的时候要用)
     *
     * @param blk 要写入的数据块
     * @param source 要别删除的数据块
     */
    public void replaceBlock(final ExtendedBlock blk,
                             final DatanodeInfo source);

}
