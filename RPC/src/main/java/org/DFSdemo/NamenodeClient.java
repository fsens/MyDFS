package org.DFSdemo;

import org.DFSdemo.conf.Configuration;
import org.DFSdemo.ipc.RPC;
import org.DFSdemo.protocol.ClientProtocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;


/** 这是客户端接口 */
public class NamenodeClient implements Closeable{
    /** 给clientRunning定义volatile关键字，为了让它的改变能被所有线程立即可见 */
    volatile boolean clientRunning = true;
    final ClientProtocol clientProtocol;

    public NamenodeClient(URI namenodeUri, Configuration conf) throws IOException {
        ServerProxies.ProxyInfo<ClientProtocol> proxyInfo = null;

        proxyInfo = ServerProxies.createProxy(conf, namenodeUri, ClientProtocol.class);
        this.clientProtocol = proxyInfo.getProxy();
    }

    public boolean rename2(String src, String dst) throws IOException {
        return this.clientProtocol.rename2(src, dst);
    }

    /**
     * 停止对ClientProtocol的代理
     */
    private void closeConnectionToNamenode(){
        RPC.stopProxy(clientProtocol);
    }

    /**
     * 关闭和Namenode的连接，释放资源
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException{
        if (clientRunning){
            clientRunning = false;
            closeConnectionToNamenode();
        }
    }

}
