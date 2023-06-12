package org.DFSdemo.protocol;

import com.sun.xml.internal.bind.v2.TODO;

/**
 *Client和Namenode之间的通讯协议
 * 对于写，读之类的具体操作，在上层方法中完成
 */
public interface ClientProtocol {
    /**
     * 得到文件指定位置的数据块
     *
     * @param src 文件名
     * @param offset 读取文件的起始位置
     * @param length 读取文件的长度
     * @return 文件指定数据块的信息
     */
    public LocatedBlocks getBlockLocations(String src , long offset , long length);

    /**
     * 创建文件
     * @param src 文件名
     * @param clientName 创建文件的用户名
     * @param masked 该文件的访问权限信息
     * @return 文件的状态
     */
    public HdfsFileStatus create(String src, String clientName, FsPermission masked);


    /**
     * 向对应文件新增一个数据块
     * @param src 文件名
     * @param clientName 添加数据块的用户名
     * @param excludeNodes 失效的数据节点名单
     * @param fileId 标识file唯一性的ID
     * @param favoredNodes 当前文件保存副本的节点列表
     * @return 新增节点的信息
     */
    public LocatedBlock addBlock(String src, String clientName,DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes);

    /**
     * 当用户要关闭文件时，调用close()方法。该方法是用来判断文件是否正常关闭的。
     * @param src 当前文件名
     * @param clientName 用户名
     * @param last 最后一个数据块的信息
     * @param fileId 标识file唯一性的ID
     * @return 返回一个布尔变量
     */
    public boolean complete(String src, String clientName,ExtendedBlock last, long fileId);

    /**
     * 对一个文件进行追加写
     *
     * @param src 文件名
     * @param clientName 用户名
     * @return 该文件最后一个数据块的信息
     */
    public LocatedBlock append(String src, String clientName);


    /**
     * 更改文件或者目录的名字
     *
     * @param src 原文件名或者原目录名
     * @param dst 更改后的名字
     */
    public void rename2(String src, String dst);


    /**
     * 创建目录
     *
     * @param src 目录名
     * @param masked 该目录的访问权限信息
     * @return 是否成功创建目录
     */
    public boolean mkdirs(String src, FsPermission masked);


    /**
     * 删除文件或者目录。如果删除目录，则会递归删除目录下面的所有子目录以及文件
     *
     * @param src 文件或者目录名
     * @return 是否成功删除
     */
    public boolean delete(String src);
}
