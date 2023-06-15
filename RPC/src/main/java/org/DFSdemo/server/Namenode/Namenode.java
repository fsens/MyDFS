package org.DFSdemo.server.Namenode;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * 和Namenode相关的一些方法
 */
public class Namenode {
    private static final String NAMENODE_URI_SCHEMA = "namenode";
    private static final int NAMENODE_URI_DEFAULT_PORT = 8866;

    /**
     * 根据host获取InetSocketAddress
     * @param host 远程连接的主机名
     * @return InetSocketAddress
     */
    public static InetSocketAddress getAddress(String host){
        return new InetSocketAddress(host, NAMENODE_URI_DEFAULT_PORT);
    }

    /**
     * 根据URI获取InetSocketAddress
     * @param namenodeUri 远程连接的URI
     * @return InetSocketAddress
     */
    public static InetSocketAddress getAddress(URI namenodeUri){
        String host = namenodeUri.getHost();
        if (host == null){
            throw new IllegalArgumentException(String.format("Invalid URI for Namenode address: %s has no host", namenodeUri.toString()));
        }
        if (!NAMENODE_URI_SCHEMA.equalsIgnoreCase(namenodeUri.getScheme())){
            throw new IllegalArgumentException(String.format("Invalid URI for Namenode address: %s is not of scheme '%s'.", namenodeUri.toString(), NAMENODE_URI_SCHEMA));
        }
        return getAddress(host);
    }
}
