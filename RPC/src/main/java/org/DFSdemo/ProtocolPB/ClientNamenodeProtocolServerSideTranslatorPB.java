package org.DFSdemo.ProtocolPB;

import org.DFSdemo.Protocol.*;

/**
 * ClientProtocol实现类与ClientNamenodeProtocolPB实现类之间的适配器，在Server上
 * 实现了ClientProtocol的方法
 */
public class ClientNamenodeProtocolServerSideTranslatorPB implements ClientProtocol {
    public LocatedBlocks getBlockLocations(String src, long offset, long length) {
        return null;
    }

    public HdfsFileStatus create(String src, String clientName, FsPermission masked) {
        return null;
    }

    public LocatedBlock addBlock(String src, String clientName, DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes) {
        return null;
    }

    public boolean complete(String src, String clientName, ExtendedBlock last, long fileId) {
        return false;
    }

    public LocatedBlock append(String src, String clientName) {
        return null;
    }

    public void rename2(String src, String dst) {

    }

    public boolean mkdirs(String src, FsPermission masked) {
        return false;
    }

    public boolean delete(String src) {
        return false;
    }

    //TODO:实现参数的反序列化
}
