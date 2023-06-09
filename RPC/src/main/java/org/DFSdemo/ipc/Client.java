package org.DFSdemo.ipc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;
import org.DFSdemo.conf.CommonConfigurationKeysPublic;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.IOUtils;
import org.DFSdemo.io.Writable;
import org.DFSdemo.ipc.protobuf.IpcConnectionContextProtos;
import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;
import org.DFSdemo.net.NetUtils;
import org.DFSdemo.protocol.RPCConstants;
import org.DFSdemo.util.ProtoUtil;
import org.DFSdemo.util.ReflectionUtils;
import org.DFSdemo.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.Constructor;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Client {

    private static final Log LOG = LogFactory.getLog("Client.class");

    /** 生成call id的计数器 */
    private static final AtomicInteger callIdCounter = new AtomicInteger();

    /** 连接的缓冲池 */
    private final Hashtable<ConnectionId, Connection> connections = new Hashtable<>();

    /** 调用的返回类型,如RpcResponseWrapper */
    private Class<? extends Writable> valueClass;
    /** 标识Client是否还在运行 */
    private AtomicBoolean running = new AtomicBoolean(true);

    private final Configuration conf;

    /** 创建socket的方式 */
    private SocketFactory socketFactory;
    private final int connectionTimeOut;
    private final byte[] clientId;//标识客户端

    /** 发送调用请求(Call对象)的线程池，这个可以将发送调用请求与其它代码隔离 */
    private final ExecutorService sendParamsExecutor;

    /**
     * @param valueClass 调用的返回类型,如RpcResponseWrapper
     * @param conf 配置对象
     * @param factory socket工厂
     */
    public Client(Class<? extends Writable> valueClass, Configuration conf, SocketFactory factory){
        this.valueClass = valueClass;
        this.conf = conf;
        this.socketFactory = factory;
        this.connectionTimeOut = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
        this.clientId = ClientId.getClientId();
        /** 这里语法上可以直接写this.sendParamsExecutor = clientExecutorFactory.clientExecutor,但是不要这样做，因为这样不能让clientExecutor引用数增加 */
        this.sendParamsExecutor = clientExecutorFactory.refAndGetInstance();
    }

    public class ClientId{
        /** UUID的字节数组长度：16 */
        public static final int BYTE_LENGTH = 16;

        public static byte[] getClientId(){
            UUID uuid = UUID.randomUUID();
            ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[BYTE_LENGTH]);//通过warp方法来创建一个新的ByteBuffer
            byteBuffer.putLong(uuid.getMostSignificantBits());
            byteBuffer.putLong(uuid.getLeastSignificantBits());
            return byteBuffer.array();
        }
    }

    /**
     * 封装了rpc调用请求和返回，组成一个rpc调用单元
     * 这是一个代表rpc调用的类
     */
    static class Call{
        final int id;
        Writable rpcRequest;
        Writable rpcResponse;
        /** 异常信息 */
        IOException error;
        final RPC.RpcKind rpcKind;
        /** 只有发出请求并得到返回值才算完成调用，默认未完成调用 */
        boolean done = false;

        private Call(RPC.RpcKind rpcKind, Writable rpcRequest){
            this.rpcKind = rpcKind;
            this.rpcRequest = rpcRequest;

            this.id = nextCallId();
        }

        /**
         * 调用完成，唤醒调用者
         * 由于调用notify()唤醒等待的线程需要先获得该锁对象，所以得加synchronized
         */
        public synchronized void callComplete(){
            this.done = true;
            //使用notify()性能更好（这里我们更关注性能而不是公平性）
            notify();
        }

        /**
         * 设置返回值
         * 由于callComplete()方法包含了notify()方法，所以得加synchronized关键字
         *
         * @param rpcResponse 调用的返回值
         */
        public synchronized void setRpcResponse(Writable rpcResponse){
            this.rpcResponse = rpcResponse;
            callComplete();
        }

        /**
         * 设置异常信息，将异常抛给上层调用者
         * 由于callComplete()方法包含了notify()方法，所以得加synchronized关键字
         *
         *
         * @param error 异常对象
         */
        public synchronized void setException(IOException error){
            this.error = error;
            callComplete();
        }

        /**
         * 返回rpc调用的返回值
         *
         * @return rpc调用的返回值
         */
        public synchronized Writable getRpcResponse(){
            return rpcResponse;
        }

    }

    /**
     * 1.返回自增的id，由于存在线程安全问题，因此counter是atomic类型的
     * 2.为了防止取负值，需要将返回结果与0x7FFFFFFF做按位与操作
     * 因此id的取值范围是[ 0, 2^32 - 1 ],当id达到最大值，会重新从0开始自增
     *
     * @return 下一个自增的id
     */
    public static int nextCallId(){
        return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
    }



    public void stop(){
        if (LOG.isDebugEnabled()){
            LOG.debug("Stopping client");
        }
        //将running设置为false，表示客户端不再运行
        if (!running.compareAndSet(true, false)){
            return;
        }

        //锁住connections并中断所有的connection线程以响应客户端的停止
        synchronized (connections){
            for (Connection connection : connections.values()){
                connection.interrupt();
            }
        }

        //等待，直到所有connection关闭
        while (!connections.isEmpty()){
            try {
                Thread.sleep(100);
            }catch (InterruptedException e){
            }
        }
        clientExecutorFactory.unrefAndCleanup();
    }

    /**
     * 该类用来存储与连接相关的address、protocol等信息，标识网络连接
     */
    public static class ConnectionId {

        final InetSocketAddress address;
        /** 一个质数，为hashCode()方法生成hash值所用 */
        private static final int PRIRME = 16777619;
        private final Class<?> protocol;
        /** rpc超时时间 */
        private final int rpcTimeOut;
        /** 连接的最大休眠时间，单位：毫秒 */
        private final int maxIdleTime;
        /** 如果为true，则禁用Nagle算法 */
        private final boolean tcpNoDelay;
        /** 是否需要发送 ping message */
        private final boolean doPing;
        /** 发送 ping message的时间间隔，时间：毫秒 */
        private final int pingInterval;
        /** socket连接超时的最大重试次数 */
        private final int maxRetriesOnSocketTimeouts;
        private final Configuration conf;

        /**
         * 构造方法，定义了一些属性来标识连接
         *
         * @param address 服务端地址
         * @param protocol 协议
         * @param rpcTimeOut 超时时间
         * @param conf 配置
         */
        public ConnectionId(InetSocketAddress address,
                            Class<?> protocol,
                            int rpcTimeOut,
                            Configuration conf){
            this.address = address;
            this.protocol = protocol;
            this.rpcTimeOut = rpcTimeOut;

            this.maxIdleTime = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.tcpNoDelay = conf.getBoolean(CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODEALY_DEFAULT);
            this.doPing = conf.getBoolean(CommonConfigurationKeysPublic.IPC_CLIENT_PING_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_PING_DEFAULT);
            this.pingInterval = doPing ? conf.getInt(CommonConfigurationKeysPublic.IPC_PING_INTERVAL_KEY,
                    CommonConfigurationKeysPublic.IPC_PING_INTERVAL_DEFAULT)
                    : 0;
            this.maxRetriesOnSocketTimeouts = conf.getInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);

            this.conf = conf;
        }

        public InetSocketAddress getAddress(){
            return address;
        }

        public Class<?> getProtocol(){
            return protocol;
        }

        public int getRpcTimeOut(){
            return rpcTimeOut;
        }

        public int getMaxIdleTime(){
            return maxIdleTime;
        }

        public boolean isTcpNoDelay(){
            return tcpNoDelay;
        }

        public boolean isDoPing(){
            return doPing;
        }

        public int getPingInterval(){
            return pingInterval;
        }

        public int getMaxRetriesOnSocketTimeouts(){
            return maxRetriesOnSocketTimeouts;
        }

        @Override
        public String toString(){
            return address.toString();
        }

        @Override
        public boolean equals(Object obj){
            if (this == obj){
                return true;
            }
            //只要核心属性相同，便认为两个连接相同，可以复用
            if (obj instanceof ConnectionId){
                ConnectionId that = (ConnectionId) obj;
                return Objects.equals(this.address, that.address)
                        && Objects.equals(this.protocol, that.protocol)
                        && this.rpcTimeOut == that.rpcTimeOut
                        && this.maxIdleTime == that.maxIdleTime
                        && this.tcpNoDelay == that.tcpNoDelay
                        && this.doPing == that.doPing
                        && this.pingInterval == that.pingInterval;
            }
            return false;
        }

        /**
         * 利用的乘法哈希算法来生成哈希值：
         * result = PRIME * result + fieldHashCode
         * @return
         */
        @Override
        public int hashCode(){
            int reuslt = ((address == null) ? 0 : address.hashCode());
            reuslt = PRIRME * reuslt + (doPing ? 1231 : 1237);
            reuslt = PRIRME * reuslt + maxIdleTime;
            reuslt = PRIRME * reuslt + pingInterval;
            reuslt = PRIRME * reuslt + ((protocol == null) ? 0 : protocol.hashCode());
            reuslt = PRIRME * reuslt + rpcTimeOut;
            reuslt = PRIRME * reuslt + (tcpNoDelay ? 1231 : 1237);
            return reuslt;
        }
    }

    /**
     * 调用RPC服务端，它相关信息定义在了remoteId中
     *
     * 这个方法是暴露给上游调用的
     *
     * @param rpcKind rpc类型（序列化/反序列化方式）。包含这个参数可以让相同rpc的传输用相同的套接字连接，不会和其它的rpc传输混淆
     * @param rpcRequest 包含序列化方法和参数
     * @param remoteId rpc server的信息
     * @return rpc返回值
     * @throws IOException
     */
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionId remoteId) throws IOException{
        return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT);
    }

    /**
     * 调用rpc服务端，相关信息定义在了remoteId中
     *
     * @param rpcKind rpc类型
     * @param rpcRequest 客户端请求，包含序列化方法和参数等信息
     * @param remoteId rpc server
     * @param serviceClass rpc的服务类
     * @return rpc返回值
     * @throws IOException 抛网络异常或者远程代码执行异常
     */
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest, ConnectionId remoteId, int serviceClass) throws IOException{
        final Call call = createCall(rpcKind, rpcRequest);
        /** 每一次call调用都会建立一次新的连接(???不应该这样啊???) */
        Connection connection = getConnection(remoteId, call, serviceClass);
        try {
            //发送rpc请求
            connection.sendRpcRequest(call);
        }catch (InterruptedException e){
            //发送调用请求被中断后，需要设置当前调用线程的中断标记位
            Thread.currentThread().interrupt();
            LOG.warn("interrupted waiting to send rpc request to server", e);
            throw new IOException(e);
        }

        boolean interrupted = false;
        //为了能在call上调用wait方法，需要在call对象上加锁
        synchronized (call){
            while (!call.done){
                try {
                    //等待，直到RPC结束被唤醒
                    call.wait();
                }catch (InterruptedException e){
                    //保存被中断过的标记
                    interrupted = true;
                }
            }
        }

        if (interrupted){
            //等待服务端返回结果时被中断，需要设置当前调用的线程的中断标记位
            Thread.currentThread().interrupt();
        }

        if (call.error != null){
            if (call.error instanceof RemoteException){
                //远程异常
                call.error.fillInStackTrace();
                throw call.error;
            }else {
                //本地异常
                InetSocketAddress address = connection.getServer();
                Class<? extends Throwable> clazz = call.error.getClass();
                try {
                    Constructor<? extends Throwable> ctor = clazz.getConstructor(String.class);
                    String msg = "Call From " + InetAddress.getLocalHost()
                            + " to " + address.getHostName() + " : " + address.getPort()
                            + " failed on exception: " + call.error;
                    Throwable t = ctor.newInstance(msg);
                    /** 将call.error指定为当前异常对象t的cause，然后抛出 */
                    t.initCause(call.error);
                    throw t;
                }catch (Throwable e){
                    LOG.warn("Unable to construct exception of type " +
                            clazz + " : it has no (String) constructor", e);
                    throw call.error;
                }
            }
        }else {
            return call.getRpcResponse();
        }
    }

    Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest){
        return new Call(rpcKind, rpcRequest);
    }

    /**
     * 利用单例的设计方法，保证只有一个线程池
     */
    private final static ClientExecutorServiceFactory clientExecutorFactory = new ClientExecutorServiceFactory();

    /**
     * 线程池工厂
     */
    private static class ClientExecutorServiceFactory{
        /** clientExecutor被引用的次数 */
        private int executorRefCount = 0;
        /** 线程池 */
        private ExecutorService clientExecutor = null;

        /**
         * 得到一个线程池
         * 由于该方法是会被多个线程同时使用的。为了保证线程安全，这里应该加锁
         * @return 线程池对象
         */
        synchronized ExecutorService refAndGetInstance(){
            if (executorRefCount == 0){
                clientExecutor = Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder().setDaemon(true)
                                .setNameFormat("IPC Parameter Sending Thread #%d")
                                .build());
            }
            executorRefCount++;
            return clientExecutor;
        }

        /**
         * 销毁线程池
         * 由于该方法是会被多个线程同时使用的。为了保证线程安全，这里应该加锁
         */
        synchronized void unrefAndCleanup(){
            executorRefCount--;
            assert executorRefCount >= 0;

            if (executorRefCount == 0){
                /** shutdown()方法会等待线程池中的任务完成再关闭线程池 */
                clientExecutor.shutdown();
                try {
                    if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)){
                        /** shutdownNow()会停止线程池的所有任务并立即关闭线程池 */
                        clientExecutor.shutdownNow();
                    }
                }catch (InterruptedException e){
                    LOG.error("Interrupted while waiting for clientExecutor" + "to stop", e);
                    clientExecutor.shutdownNow();
                }
                clientExecutor = null;
            }
        }
    }

    private class Connection extends Thread{

        private final ConnectionId remoteId;
        private InetSocketAddress server;

        /** rpc超时时间 */
        private final int rpcTimeOut;
        /** 连接的最大休眠时间，单位：毫秒 */
        private final int maxIdleTime;
        /** 如果为true，则禁用Nagle算法 */
        private final boolean tcpNoDelay;
        /** 是否需要发送 ping message */
        private final boolean doPing;
        /**
         * 发送 ping message的时间间隔,时间：毫秒
         * 这里没有定义为final是因为后面需要根据情况覆写pingInterval
         */
        private int pingInterval;
        /** socket连接超时的最大重试次数 */
        private final int maxRetriesOnSocketTimeouts;
        private int serviceClass;
        /** 关闭的原因 */
        private IOException closeException;

        /** 标识是否应该关闭连接，默认值：false */
        private AtomicBoolean shouldCloseConnection = new AtomicBoolean();

        /** IO活动的最新时间 */
        private AtomicLong lastActivity = new AtomicLong();

        /** 该网络连接需要处理的所有RPC调用单元 */
        private Hashtable<Integer, Call> calls = new Hashtable<>();

        /** 该连接的套接字 */
        private Socket socket = null;

        /** 输入流和输出流 */
        private DataInputStream in;
        private DataOutputStream out;

        public Connection(ConnectionId remoteId, Integer serviceClass) throws IOException{
            this.remoteId = remoteId;
            this.server = remoteId.getAddress();
            this.serviceClass = serviceClass;
            if ((server.isUnresolved())){
                throw new UnknownHostException("Unknown host name:" + server.toString());
            }
            this.rpcTimeOut = remoteId.getRpcTimeOut();
            this.maxIdleTime = remoteId.getMaxIdleTime();
            this.tcpNoDelay = remoteId.isTcpNoDelay();
            this.doPing = remoteId.isDoPing();
            if (doPing){

            }

            this.pingInterval = remoteId.getPingInterval();
            this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
            this.serviceClass = serviceClass;
            if (LOG.isDebugEnabled()){
                LOG.debug("The ping interval is " + this.pingInterval + "ms.");
            }

            this.setName("TCP Client (" + socketFactory.hashCode() + ") connection to" +
                    server.toString());
            //设置为守护线程
            this.setDaemon(true);
        }

        public InetSocketAddress getServer(){
            return server;
        }

        /**
         * 判断服务端是否有响应，如果有则接收响应并解析，没有就关闭连接
         */
        @Override
        public void run(){
            if (LOG.isDebugEnabled()){
                LOG.debug(getName() + ": starting, having connections " + connections.size());
            }

            try {
                while (waitForWork()){
                    receiveRpcResponse();
                }
            }catch (Throwable t){
                LOG.warn("Unexpected error reading responses on connection " + this, t);
                markClosed(new IOException("Error reading responses", t));
            }

            close();

            if (LOG.isDebugEnabled()){
                LOG.debug(getName() + ": stopped, remaining connections " + connections.size());
            }

        }

        /**
         * 关闭连接
         */
        private synchronized void close(){
            if (!shouldCloseConnection.get()){
                LOG.error("The connection is not in the closed state.");
                return;
            }

            //释放连接资源
            synchronized (connections){
                if (connections.get(remoteId) == this){
                    connections.remove(remoteId);
                }
            }

            //关闭输入输出流
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);

            if (closeException == null){
                if (!calls.isEmpty()){
                    LOG.warn("A ocnnection is closed for no cause and calls are not empty.");
                    closeException = new IOException("Unexpected closed connection");
                    cleanupCalls();
                }
            }else {
                if (LOG.isDebugEnabled()){
                    LOG.debug("closing ipc connection to " + server + ": " +
                            closeException.getMessage(), closeException);
                }
                cleanupCalls();
                if (LOG.isDebugEnabled()){
                    LOG.debug(getName() + ":closed");
                }
            }
        }

        /**
         * remove所有的call，并设置为本地异常
         */
        private void cleanupCalls(){
            Iterator<Map.Entry<Integer, Call>> iter = calls.entrySet().iterator();
            while (iter.hasNext()){
                Call call = iter.next().getValue();
                iter.remove();
                call.setException(closeException);
            }
        }

        /**
         * 向该Connection对象的Call队列中加入一个call
         * 同时唤醒一个在this上等待的线程
         *
         * @param call 加入待处理call队列的元素
         * @return 如果连接处于关闭状态，返回false；如果call正确加入了队列，则返回true
         */
        private synchronized boolean addCall(Call call){
            if (shouldCloseConnection.get()){
                return false;
            }
            calls.put(call.id, call);
            //随机唤醒一个在this上等待的线程，使用notify性能更好（这里我们更关注性能而不是公平性）
            //在本程序中，实际上只有waitForWork()方法才在connection对象上调用了wait()方法，所以这实际上是唤醒connection线程(接收线程)的
            notify();
            return true;
        }

        /**
         * 建立完整的IO流流程：
         * 1.连接server
         * 2.建立IO流
         * 3.向server发送header/context信息
         * 4.启动receiver线程
         *
         * 由于多个线程持有相同的Connection对象，需要保证只有一个线程可以执行上述业务逻辑
         * 因此该方法需要用synchronized修饰
         */
        private synchronized void setupIOStream(){
            /**
             * 如果socket不为空，则说明上一次关闭连接{@link Connection#closeConnection()}时出现了异常
             * 所以该连接暂时不能使用
             */
            if (socket != null || shouldCloseConnection.get()){
                return;
            }

            try {
                if (LOG.isDebugEnabled()){
                    LOG.debug("Connection to " + server);
                }
                /** 1.连接server */
                setupConnection();
                /** 2.建立IO流 */
                InputStream inStream = NetUtils.getInputStream(socket);
                OutputStream outStream = NetUtils.getOutputStream(socket);
                /** 3.向server发送header信息 */
                writeConnectionHeader(outStream);

                if (doPing){
                    /**
                     * ping相关
                     */
                }

                /**
                 * 包装Connection类的输入输出流
                 * DataInputStream(DataOutputStream)可以支持Java原子类的输入(输出)
                 * BufferedInputStream(BufferedOutputStream)具有缓冲作用
                 */
                this.in = new DataInputStream(new BufferedInputStream(inStream));
                this.out = new DataOutputStream(new BufferedOutputStream(outStream));

                /** 3.向server发送context信息 */
                writeConnectionContext(remoteId);

                touch();

                /** 4.启动receiver线程，用来接收响应信息 */
                start();
                return;
            }catch (Throwable t){
                if (t instanceof IOException){
                    markClosed((IOException) t);
                }else {
                    markClosed(new IOException("Couldn't set up IO stream", t));
                }
            }
            close();
        }

        /**
         * 将当前时间更新为I/O最新活动时间
         */
        private void touch(){
            lastActivity.set(System.currentTimeMillis());
        }

        /**
         * 标志网络连接状态为关闭
         *
         * @param e 关闭的原因
         */
        private synchronized void markClosed(IOException e){
            if (shouldCloseConnection.compareAndSet(false, true)){
                closeException = e;
                notifyAll();
            }
        }

        /**
         * 建立socket连接
         *
         * 可能会有多个线程共享该连接，所以要保证同一时刻只能有一个线程建立与服务端的连接
         * 该方法以this为锁
         *
         * @throws IOException
         */
        private synchronized void setupConnection() throws IOException{
            short timeOutFailures = 0;
            while (true){
                try {
                    this.socket = socketFactory.createSocket();
                    this.socket.setTcpNoDelay(tcpNoDelay);
                    this.socket.setKeepAlive(true);

                    NetUtils.connect(socket, server, connectionTimeOut);

                    if (rpcTimeOut > 0){
                        //用rpcTimeOut覆盖pingInterval
                        pingInterval = rpcTimeOut;
                    }
                    socket.setSoTimeout(pingInterval);
                    return;
                }catch (SocketTimeoutException ste){
                    handleConnectionTimeout(timeOutFailures++, maxRetriesOnSocketTimeouts, ste);
                }catch (IOException ioe){
                    throw ioe;
                }
            }
        }

        /**
         * 连接超时的处理方法
         *
         * @param curRetries 当前重连接次数
         * @param maxRetries 最大重连接次数
         * @param ioe 抛出的套接字连接超时异常
         * @throws IOException
         */
        private void handleConnectionTimeout(int curRetries, int maxRetries, IOException ioe) throws IOException{
            closeConnection();

            if (curRetries >= maxRetries){
                throw ioe;
            }
            LOG.info("Retrying connect to server: " + server + ".Already tried"
            + curRetries + "time(s);maxRetries=" + maxRetries);
        }

        /**
         * 关闭套接字连接
         */
        private void closeConnection(){
            if (socket == null){
                return;
            }
            try {
                socket.close();
            }catch (IOException e){
                e.printStackTrace();
                LOG.warn("Not able to close a socket",e);
            }
            //将socket置为null，为了下次能够重新建立连接
            socket = null;
        }

        /**
         * 建立连接后发送的请求头（header）
         * +----------------------------+
         * |"myrpc" 5 字节               |
         * +----------------------------+
         * |Service Class 1 字节         |
         * +----------------------------+
         * |AuthProtocol 1 字节         |
         *
         * @param outStream 输出流
         * @throws IOException
         */
        private void writeConnectionHeader(OutputStream outStream) throws IOException{
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));

            out.write(RPCConstants.HEADER.array());
            out.write(serviceClass);
            //暂无授权协议，写0
            out.write(0);

            out.flush();
        }

        /**
         * 每次连接都要写连接上下文
         *
         * @param remoteId 服务端地址
         * @throws IOException
         */
        private void writeConnectionContext(ConnectionId remoteId) throws IOException{
            /** 构造连接上下文对象 */
            IpcConnectionContextProtos.IpcConnectionContextProto connectionContext =
                    ProtoUtil.makeIpcConnectionContext(RPC.getProtocolName(remoteId.getProtocol()));

            RpcHeaderProtos.RpcRequestHeaderProto connectionContextHeader =
                    ProtoUtil.makeRpcRequestHeader(
                    RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                            RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
                            RPCConstants.CONNECTION_CONTEXT_CALL_ID,
                            clientId,
                            RPCConstants.INVALID_RETRY_COUNT);
            /** 包装连接上下文对象 */
           ProtobufRpcEngine.RpcRequestMessageWrapper request = new ProtobufRpcEngine.RpcRequestMessageWrapper(
                   connectionContextHeader, connectionContext
           );
            /** 向该连接的输出流中写入序列化后的连接上下文的总长度
             * 以及序列化后的连接上下文
             */
            out.writeInt(request.getLength());
            request.write(out);
        }

        /** 创建发送调用请求的锁。该锁是为了保证多个进程/线程持有相同的Client对象时对该锁锁上的代码块的访问是串行的 */
        private final Object sendRpcRequestLock = new Object();

        /**
         * 向服务端发送rpc请求
         *
         * @param call 包含rpc调用的相关信息
         * @throws IOException
         * @throws InterruptedException
         */
        public void sendRpcRequest(final Call call) throws IOException, InterruptedException{
            if (shouldCloseConnection.get()){
                return;
            }

            /**
             * 序列化需要发送出去的消息，这里由实际调用方法的线程来完成
             * 实际发送前各个线程可以并行地准备（序列化）待发送地信息，而不是发送线程(sendParamExecutor)
             * 这样做的好处：1.可以减小锁的细粒度；2.序列化过程中抛出的异常每个线程可以单独、独立地报告
             *
             * 发送的格式：
             * 0)下面1、2两项的长度之和，4个字节
             * 1)RpcRequestHeader
             * 2)RpcRequest
             */
            final ByteArrayOutputStream bo = new ByteArrayOutputStream();
            final DataOutputStream tmpOut = new DataOutputStream(bo);
            /** 暂时没有重试机制，所以retryCount=-1 */
            RpcHeaderProtos.RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
                    call.rpcKind, RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET,
                    call.id,clientId,-1);
            /** 写入header到临时的输出流中 */
            header.writeDelimitedTo(tmpOut);
            /** 写入请求调用的信息到临时的输出流中 */
            call.rpcRequest.write(tmpOut);

            /** 保证持有相同Client对象的进程/线程对该代码块的访问是串行的 */
            synchronized (sendRpcRequestLock){
                Future<?> senderFuture = sendParamsExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        /** 多线程并发调用服务端，需要锁住输出流out，防止冲突 */
                        try {
                            //由于这是内部类(匿名内部类),访问外部类的非静态属性的方法是:外部类.this.属性名
                            //其他线程可能在使用out写header
                            synchronized (Connection.this.out){
                                if (shouldCloseConnection.get()){
                                    return;
                                }
                                if (LOG.isDebugEnabled()){
                                    LOG.debug(getName() + "sending #" + call.id);
                                }

                                byte[] data = bo.toByteArray();
                                int dataLen = bo.size();
                                out.writeInt(dataLen);
                                out.write(data, 0, dataLen);
                                out.flush();
                            }
                        }catch (IOException e){
                            /**
                             * 如果在这里发生异常，将处于不可恢复状态
                             * 因此，关闭连接，终止所有未完成的调用
                             */
                            markClosed(e);
                        }finally {
                            IOUtils.closeStream(tmpOut);
                        }
                    }
                });

                try {
                    senderFuture.get();
                }catch (ExecutionException e){
                    //Java有异常链，该异常可能是由另一个异常引起的
                    //调用getCause方法获取真正的异常
                    Throwable cause = e.getCause();

                    /**
                     * 这里只能是运行时异常，因为IOException异常已经在上面的匿名内部类捕获了
                     */
                    if (cause instanceof RuntimeException){
                        throw (RuntimeException) cause;
                    }else {
                        throw new RuntimeException("unexpected checked exception", cause);
                    }
                }
            }
        }

        /**
         * 判断是否有服务端响应可接收
         * @return
         */
        private synchronized boolean waitForWork(){
            if (calls.isEmpty() && !shouldCloseConnection.get() && running.get()){
                //计算等待时间
                long timeout = maxIdleTime - (System.currentTimeMillis() - lastActivity.get());
                if (timeout > 0){
                    try {
                        //如果calls为空，则先等待一会时间
                        wait(timeout);
                    }catch (InterruptedException e){
                        //被中断属于正常现象，无需报警
                    }
                }
            }
            if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()){
                return true;
            }else if (shouldCloseConnection.get()){
                return false;
            }else if (calls.isEmpty()){
                //没有call了，需要终止连接
                markClosed(null);
                return false;
            }else {
                //running状态已经为false，但仍然有call
                markClosed(new IOException("", new InterruptedException()));
                return false;
            }
        }

        /**
         * 接收、解析服务端响应
         */
        private void receiveRpcResponse(){
            if (shouldCloseConnection.get()){
                return;
            }
            touch();

            try {
                //读取响应信息的总长度
                int totalLen = in.read();
                //从响应信息里面反序列化header
                RpcHeaderProtos.RpcResponseHeaderProto header =
                        RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(in);
                //check header的正确性
                checkResponse(header);

                //计算header占用的总长度，包括数据本身的长度以及长度的varint32编码后的长度
                int headerLen = header.getSerializedSize();
                headerLen += CodedOutputStream.computeUInt32SizeNoTag(headerLen);

                int callId = header.getCallId();
                if (LOG.isDebugEnabled()){
                    LOG.debug(getName() + "got value #" + callId);
                }

                Call call = calls.get(callId);
                RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto status = header.getStatus();
                //判断RPC调用是否成功
                if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS){
                    Writable value = ReflectionUtils.newInstance(valueClass);
                    //响应头header已经被从输入流in中读出，现在可以读调用方法的返回值了
                    value.readFields(in);
                    calls.remove(callId);
                    call.setRpcResponse(value);
                }else {
                    /**
                     * 对于RPC调用错误信息，需要获取具体的错误类、错误码、错误信息和栈踪
                     */
                    if (totalLen != headerLen){
                        throw new RpcClientException(
                                "RPC response length mismatch on rpc error");
                    }

                    String exceptionClassName = header.hasExceptionClassName() ? header.getExceptionClassName() : "ServerDidNotSetExceptionClassName";
                    String errMsg = header.hasErrorMsg() ? header.getErrorMsg() : "ServerDidNotSetErrorMsg";
                    final RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errCode =
                            header.hasErrorDetail() ? header.getErrorDetail() : null;
                    if (errCode == null){
                        LOG.warn("Detailed error code not set by server on rpc error");
                    }
                    RemoteException re = new RemoteException(exceptionClassName, errMsg, errCode);
                    if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR){
                        //对于非致命错误，报告异常值，不必关闭连接
                        calls.remove(callId);
                        call.setException(re);
                    }else {
                        //致命错误，需要关闭当前连接
                        markClosed(re);
                    }
                }
            }catch (IOException e){
                markClosed(e);
            }
        }

    }

    /**
     * 判断服务端返回的ClientId对象与当前Client对象的clientId是否相等
     *
     * @param header 反序列化后的响应头
     * @throws IOException
     */
    void checkResponse(RpcHeaderProtos.RpcResponseHeaderProto header) throws IOException{
        if (header == null){
            throw new IOException("Response is null");
        }

        if (header.hasCallId()){
            final byte[] responseId = header.getClientId().toByteArray();
            if (!Arrays.equals(responseId, RPCConstants.DUMMY_CLIENT_ID)){
                if (!Arrays.equals(responseId, clientId)){
                    throw new IOException("Client ID is not matched: local ID="
                    + StringUtils.byteToHexString(clientId) + ", ID in response="
                    + StringUtils.byteToHexString(header.getClientId().toByteArray()));
                }
            }
        }
    }

    /**
     * 从缓冲池中获取一个Connection对象，如果池中不存在，需要创建对象并放入缓冲池
     *
     * @param remoteId ConnectionId
     * @param call 一次调用
     * @param serviceClass 服务类的标识符
     * @return 一个连接
     * @throws IOException
     */
    private Connection getConnection(ConnectionId remoteId, Call call, int serviceClass) throws IOException{
        if (!running.get()){
            throw new IOException("The client is stopped.");
        }
        Connection connection;
        do {
            synchronized (connections){
                connection = connections.get(remoteId);
                if (connection == null){
                    connection = new Connection(remoteId, serviceClass);
                    connections.put(remoteId, connection);
                }
            }
        }while (!connection.addCall(call));

        //我们没有在上面synchronized(connections)代码块调用该方法
        //原因是如果服务端慢，建立连接会花费很长时间，会拖慢整个系统
        connection.setupIOStream();
        return connection;
    }


}
