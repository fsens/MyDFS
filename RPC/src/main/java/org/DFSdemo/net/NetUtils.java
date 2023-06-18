package org.DFSdemo.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class NetUtils {

    private static Log LOG = LogFactory.getLog(NetUtils.class);

    /**
     * {@link Socket#connect(SocketAddress, int)}的替代方法
     * 该方法检查了一些连接异常，更加地健壮
     *
     * @param socket 套接字
     * @param endpoint 服务端地址
     * @param timeout 超时时间
     * @throws IOException
     */
    public static void connect(Socket socket, InetSocketAddress endpoint, int timeout) throws IOException{
        if (socket == null || endpoint == null || timeout < 0){
            throw new IllegalArgumentException("Illegal Argument for connect");
        }
        socket.connect(endpoint, timeout);

        /**
         * TCP可能会出现一种罕见的情况--自连接。
         * 1.在服务端down掉的情况下，操作系统会为客户端进程随机分配一个端口，与服务端重连。当分配到和服务端进程相同的端口号时，客户端连接本地的服务端有可能连接成功
         * 但是客户端的地址和端口与服务端的相同。这会导致本地再次启用服务端失败，因为端口和地址被占用。
         * 2.当本地客户端进程与远程的服务端进程通信时，防火墙可能会将远程同一地址的节点的流量重定向到本地回环接口。这也可能导致本地的TCP自连接。
         *
         * 因此，出现这种情况视为连接拒绝。
         */
        if (socket.getLocalAddress().equals(socket.getInetAddress()) &&
        socket.getLocalPort() == socket.getPort()){
            LOG.info("Detected a loopback TCP socket,disconnection it");
            socket.close();
            throw new ConnectException("Localhost targeted connection resulted in a loopback." +
                    "No daemon is listening on the target port.");
        }
    }

    /**
     * 获取套接字默认的输入流
     *
     * @param socket
     * @return 套接字默认的输入流
     * @throws IOException
     */
    public static InputStream getInputStream(Socket socket) throws IOException{
        return socket.getInputStream();
    }

    /**
     * 获取套接字默认的输出流
     *
     * @param socket
     * @return 套接字默认的输出流
     * @throws IOException
     */
    public static OutputStream getOutputStream(Socket socket) throws IOException{
        return socket.getOutputStream();
    }
}
