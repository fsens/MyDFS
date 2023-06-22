package org.DFSdemo.server.Namenode;

import org.DFSdemo.protocol.ClientProtocol;

import java.io.IOException;

/**
 * 该类是ClientProtocol接口的实现类，由名字节点完成
 */
public class ClientProtocolImpl implements ClientProtocol {

    @Override
    public boolean rename2(String src, String dst) throws IOException {
        //TODO:Namenode实现rename2
        return false;
    }
}
