package org.DFSdemo.ipc;

import org.DFSdemo.conf.CommonConfigurationKeysPublic;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.Writable;
import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;
import org.DFSdemo.protocol.RPCConstants;
import org.DFSdemo.util.ProtoUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Server {

    public static final Log LOG = LogFactory.getLog(Server.class);
    private String bindAddress;
    private int port;
    private int handlerCount;
    private int readThreads;

    /**
     * callQueue队列的大小
     */
    private int maxQueueSize;

    /**
     * 响应buffer的上限
     */
    private int maxRespSize;

    /**
     * 客户端信息长度的上限
     */
    private int maxDataLength;
    private boolean tcpNoDelay;

    /**
     * pendingConnections队列的大小
     */
    private int readerPendingConnectionQueue;

    /**
     * 存放已经解析好的客户端请求的队列
     */
    private BlockingDeque<Call> callQueue;

    volatile private boolean running = true;
    private ConnectionManager connectionManager;
    private Listener listener;
    private Responder responder;
    private Handler[] handlers = null;
    private Configuration conf;

    /** 当读写buffer的大小超过8KB限制，读写操作的数据将按照该值分隔
     *  大部分RPC不会超过这个值
     */
    private static int NIO_BUFFER_LIMIT = 8*1024;

    /**
     * 该函数是{@link ReadableByteChannel#read(ByteBuffer)}的wrapper
     * 如果需要读数据量较大，可以分成从channel读数据。这样，可以避免
     * 随着ByteBuffer大小的增加，jdk创建大量的buffer，从而避免性能衰减
     *
     * @see ReadableByteChannel#read(ByteBuffer)
     *
     * @param channel 通道
     * @param buffer
     * @return 如果读取成功，则返回读取或者写入的字节数；如果失败，返回0或者-1
     * @throws IOException
     */
    private int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException{
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.read(buffer) : channelIO(channel, null, buffer);
        return count;
    }

    /**
     * 该函数是{@link WritableByteChannel#write(ByteBuffer)}的wrapper
     * 如果需要读数据量较大，可以分成从channel写数据。这样，可以避免
     * 随着ByteBuffer大小的增加，jdk创建大量的buffer，从而避免性能衰减
     *
     * @see WritableByteChannel#write(ByteBuffer)
     *
     * @param channel 通道
     * @param buffer
     * @return 如果读取成功，则返回读取或者写入的字节数；如果失败，返回0或者-1
     * @throws IOException
     */
    private int channelWrite(WritableByteChannel channel, ByteBuffer buffer) throws IOException{
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ? channel.write(buffer) : channelIO(null, channel, buffer);
        return count;
    }

    /**
     * 通道的分批读写，分批的单位是NIO_BUFFER_LIMIT
     * readCh和writeCh仅有一个非空，当readCh非空代表读通道，当writeCh非空代表写通道
     *
     * @param readCh 读通道
     * @param writeCh 写通道
     * @param buf 缓冲区
     * @return 如果读取成功，则返回读取或者写入的字节数；如果失败，返回0或者-1
     * @throws IOException
     */
    private static int channelIO(ReadableByteChannel readCh, WritableByteChannel writeCh, ByteBuffer buf) throws IOException{
        int originalLimit = buf.limit();
        int initialRemaining = buf.remaining();
        int ret = 0;
        while (buf.remaining() > 0){
            try {
                int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
                buf.limit(buf.position() + ioSize);

                ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

                if (ret < ioSize){
                    /** 说明管道中的数据已经读取完或者无法写入了，无需再循环 */
                    break;
                }
            }finally {
                //将limit恢复初始值，以进行下一次的读写
                buf.limit(originalLimit);
            }
        }

        int nBytes = initialRemaining - buf.remaining();
        return (nBytes > 0) ? nBytes : ret;
    }

    /**
     * Server类的构造方法
     *
     * @param bindAddress 服务端地址
     * @param port 服务端接口
     * @param numHandlers Handler线程的数量
     * @param numReaders Reader线程的数量
     * @param queueSizePerHandler 每个Handler期望的消息队列大小， 再根据numHandlers可以得出队列总大小
     * @param conf 配置
     * @throws IOException
     */
    protected Server(String bindAddress, int port,
                     int numHandlers, int numReaders, int queueSizePerHandler,
                     Configuration conf) throws IOException{
        this.conf = conf;
        this.bindAddress = bindAddress;
        this.port = port;
        this.handlerCount = numHandlers;

        if (numReaders != -1){
            this.readThreads = numReaders;
        }else {
            this.readThreads = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_THREADS_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
        }
        if (queueSizePerHandler != -1){
            this.maxQueueSize = numHandlers * queueSizePerHandler;
        }else {
            this.maxQueueSize = numHandlers * conf.getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
        }
        this.maxDataLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_MAXIMUM_DATA_LENGTH,
                CommonConfigurationKeysPublic.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
        this.maxRespSize = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
        this.readerPendingConnectionQueue = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);
        this.callQueue = new LinkedBlockingDeque<>(maxQueueSize);

        //创建listener
        this.listener = new Listener();
        this.connectionManager = new ConnectionManager();

        this.tcpNoDelay = conf.getBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODEALY_DEFAULT);

        //创建responder
        this.responder = new Responder();
    }

    /**
     * 启动服务
     * 1.启动Listener（Reader线程由Listener线程启动）
     * 2.启动Responder
     * 3.创建Handler线程并启动
     */
    public void start(){
        listener.start();
        responder.start();

        handlers = new Handler[handlerCount];
        for (int i = 0; i < handlerCount; i++){
            handlers[i] = new Handler(i);
            handlers[i].start();
        }
    }

    public synchronized void join() throws InterruptedException{
        while (running){
            wait();
        }
    }


    /** 保存RPC类型与RpcKindMapValue的对应关系 */
    static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>();

    /**
     * 作为rpcKindMap的value，封装了调用请求的封装类和方法调用类
     */
    static class RpcKindMapValue{
        final Class<? extends Writable> rpcRequestWrapperClass;
        final RPC.RpcInvoker rpcInvoker;

        RpcKindMapValue(Class<? extends Writable> rpcRequestWrapperClass, RPC.RpcInvoker rpcInvoker){
            this.rpcRequestWrapperClass = rpcRequestWrapperClass;
            this.rpcInvoker = rpcInvoker;
        }
    }

    /**
     * 将RpcKind对象和RpcKindMapValue对象组成的键值对写入rpcKindMap集合
     *
     * @param rpcKind RPC.RpcKind对象
     * @param rpcRequestWrapperClass 调用请求的封装类的Class对象
     * @param rpcInvoker 服务端方法调用类对象
     */
    static void registerProtocolEngine(RPC.RpcKind rpcKind,
                                       Class<? extends Writable> rpcRequestWrapperClass,
                                       RPC.RpcInvoker rpcInvoker){
        RpcKindMapValue rpcKindMapValue = new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker);
        RpcKindMapValue old = rpcKindMap.put(rpcKind, rpcKindMapValue);
        if (old != null){
            rpcKindMap.put(rpcKind, old);
            throw new IllegalArgumentException("ReRegistration of rpcKind:" + rpcKind);
        }
        LOG.debug("rpcKind=" + rpcKind +
                ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
                ",rpcInvoker=" + rpcInvoker);
    }

    /**
     * socket监听线程
     */
    private class Listener extends Thread{

        private ServerSocketChannel acceptChannel;
        private Selector selector;
        private Reader[] readers;
        //当前第几个Reader
        private int currentReader = 0;
        private InetSocketAddress address;
        /** 监听队列的长度 */
        private int backlogLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);

        public Listener() throws IOException{
            this.address = new InetSocketAddress(bindAddress, port);
            /** 创建服务端socket，并设置为非阻塞 */
            this.acceptChannel = ServerSocketChannel.open();
            this.acceptChannel.configureBlocking(false);

            /** 将服务端socket绑定到主机和端口 */
            bind(acceptChannel.socket(), address, backlogLength);
            /** 创建selector */
            this.selector = Selector.open();
            /** 注册selector */
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

            /** 创建reader线程并启动 */
            this.readers = new Reader[readThreads];
            for (int i = 0; i < readThreads; i++ ){
                Reader reader = new Reader("Socket Reader #" + (i + 1) + "for port" + port);
                this.readers[i] = reader;
                reader.start();
            }

            this.setName("IPC Server listener on" + port);
            /** 设置为daemon，它的线程也是daemon */
            this.setDaemon(true);
        }

        @Override
        public void run(){
            LOG.info("Listener thread" + Thread.currentThread().getName() + ": starting");
            connectionManager.startIdleScan();
            while (running){
                SelectionKey key = null;
                try {
                    /** 阻塞等待，直到1.至少有一个通道准备好I/O操作或者2.被中断 */
                    selector.select();
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()){
                        key = iterator.next();
                        iterator.remove();
                        if (key.isValid()){
                            /** 判断是不是OP_ACCEPT事件 */
                            if (key.isAcceptable()){
                                System.out.println("client accept");
                                doAccept(key);
                            }
                        }
                        key = null;
                    }
                }catch (OutOfMemoryError oom){
                    //out of memory 时，需要关闭所有空闲连接
                    LOG.warn("Out of Memory in server select", oom);
                    //关闭当前连接
                    closeCurrentConnection(key);
                    connectionManager.closeIdle(true);
                    try {Thread.sleep(60000);}catch (InterruptedException e){}
                }catch (Exception e){
                    closeCurrentConnection(key);
                }
            }

            LOG.info("Stopping " + Thread.currentThread().getName());

            try {
                acceptChannel.close();
                selector.close();
            }catch (IOException e){
                LOG.warn("Ignoring listener close exception", e);
            }

            acceptChannel = null;
            selector = null;

            connectionManager.startIdleScan();
            connectionManager.closeAll();
        }

        /**
         * 处理OP_ACCEPT事件
         *
         * @param key 响应OP_ACCEPT事件的通道对应的SelectionKey对象
         * @throws IOException
         * @throws InterruptedException
         */
        void doAccept(SelectionKey key) throws IOException, InterruptedException{
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel;
            while ((channel = server.accept()) != null){
                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(true);
                channel.socket().setKeepAlive(true);

                /** 为channel创建一个Connection对象 */
                Connection conn = connectionManager.register(channel);
                /** 将创建的Connection对象conn附着到key上，方便以后根据key获取该对象 */
                key.attach(conn);

                Reader reader = getReader();
                reader.addConnection(conn);
            }
        }

        /**
         * 读取客户端请求
         *
         * @param key 注册到选择器的事件
         * @throws InterruptedException 写入阻塞队列callQueue时可能阻塞等待，阻塞等待的时候可能被中断
         */
        void doRead(SelectionKey key) throws InterruptedException{
            int count;
            Connection conn = (Connection) key.attachment();
            if (conn == null){
                return;
            }
            conn.setLastContact(System.currentTimeMillis());

            try {
                count = conn.readAndProcess();
            }catch (InterruptedException ie){
                LOG.info(Thread.currentThread().getName() +
                        "readAndProcess caught Interrupted", ie);
                throw ie;
            }catch (Exception e){
                /** 这层进行异常的捕获，因为WrappedRpcServerException异常已经发送响应信息给客户端了，无需记录栈踪 */
                /** 但是其他异常属于服务器内部异常，需要记录 */
                LOG.info(Thread.currentThread().getName() + ": readAndProcess from client " +
                                conn.getHostAddress() + "throw exception [" + e + "]",
                        (e instanceof WrappedRpcServerException) ? null : e);
                //为了关闭连接，将count设置为-1
                count = -1;
            }
            if (count < 0){
                closeConnection(conn);
                conn = null;
            }else {
                conn.setLastContact(System.currentTimeMillis());
            }
        }

        /**
         * 关闭当前连接
         *
         * @param key 当前连接的通道对应的SelectionKey对象
         */
        private void closeCurrentConnection(SelectionKey key){
            if (key != null){
                Connection conn = (Connection) key.attachment();
                if (conn != null){
                    closeConnection(conn);
                    conn = null;
                }
            }
        }

        private void closeConnection(Connection coon){
            connectionManager.close(coon);
        }

        Reader getReader(){
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
        }

        private class Reader extends Thread{
            //线程安全的阻塞队列
            private final BlockingQueue<Connection> pendingConnections;
            private final Selector readSelector;

            Reader(String name) throws IOException{
                super(name);
                this.pendingConnections = new LinkedBlockingDeque<>(readerPendingConnectionQueue);
                readSelector = Selector.open();
            }

            /**
             * 生产者将connection入队列，唤醒readSelector
             *
             * @param conn 一个新的connection
             * @throws InterruptedException 当队列满了的时候put操作会阻塞，阻塞过程中可能被中断，抛出InterruptedException
             */
            void addConnection(Connection conn) throws InterruptedException{
                pendingConnections.put(conn);
                /** 唤醒readSelector */
                readSelector.wakeup();
            }

            @Override
            public void run(){
                LOG.info("Starting " + Thread.currentThread().getName());
                try {
                    doRunLoop();
                }finally {
                    try {
                        readSelector.close();
                    }catch (IOException ioe){
                        LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
                    }
                }
            }

            /**
             * 不断消费pendingConnections队列的内容并注册SelectionKey.OP_READ事件
             */
            void doRunLoop(){
                while (running){
                    SelectionKey key = null;
                    try {
                        //只消费当前已入队列的Connection对象
                        //应该避免在队列上阻塞等待，导致后面select方法得不到调用
                        int size = pendingConnections.size();
                        for (int i = 0; i < size; i++){
                            Connection conn = pendingConnections.take();
                            /** 每个connection的channel注册OP_READ事件到readSelector上 */
                            /** 第三个参数是“附着物”，附着到key上，以后可以通过key.attachment()获得 */
                            conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
                        }
                        readSelector.select();

                        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                        while (iter.hasNext()){
                            key = iter.next();
                            iter.remove();
                            if (key.isValid() && key.isReadable()){
                                doRead(key);
                            }
                            key = null;
                        }
                    }catch (InterruptedException e){
                        if (running){
                            LOG.info(Thread.currentThread().getName() + "unexpectedly interrupted", e);
                        }
                    }catch (IOException e){
                        LOG.error("Error in Reader", e);
                    }
                }
            }
        }

    }

    /**
     * 封装socket与address绑定的代码，为了在这层做异常的处理
     *
     * @param socket 服务端socket
     * @param address 需要绑定的地址
     * @param backlog 服务端监听连接请求的队列长度
     * @throws IOException
     */
    public static void bind(ServerSocket socket, InetSocketAddress address, int backlog) throws IOException{
        try {
            socket.bind(address, backlog);
        }catch (SocketException se){
            throw new IOException("Failed on local exception:"
                    + se
                    + "; bind address: " + address, se);
        }
    }

    /**
     * 处理队列中的调用请求
     */
    private class Handler extends Thread{
        Handler(int instanceNumber){
            this.setDaemon(true);
            this.setName("IPC Server handler" + instanceNumber + "on" + port);
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread{

    }

    private class Connection{
        /** 连接的通道 */
        private SocketChannel channel;
        /** 网络连接的最新活动时间 */
        private volatile long lastContact;
        private Socket socket;
        private InetAddress remoteAddr;
        private String hostAddress;
        private int remotePort;
        /** 网络连接正在处理的RPC请求数量 */
        private volatile int rpcCount;
        /** 建立连接后发送的头信息中的第一个，即myrpc */
        private ByteBuffer connectionHeaderHeaderBuf = null;
        /** 建立连接后发送的头信息中的剩下的 */
        private ByteBuffer connectionHeaderBuf = null;
        /** 四字节长度 */
        private ByteBuffer dataLengthBuffer = null;
        /** 存放数据的缓冲区 */
        private ByteBuffer data = null;
        /** 连接头是否被读过 */
        private boolean connectionHeaderRead = false;
        /** 连接头后的连接上下文是否被读过 */
        private boolean connectionContextRead = false;

        Connection(SocketChannel channel, long lastContact){
            this.channel = channel;
            this.lastContact = lastContact;
            this.socket = channel.socket();
            this.remoteAddr = socket.getInetAddress();
            this.hostAddress = remoteAddr.getHostAddress();
            this.remotePort = socket.getPort();
            this.dataLengthBuffer = ByteBuffer.allocate(5);
        }

        private void setLastContact(long lastContact){
            this.lastContact = lastContact;
        }

        private long getLastContact(){
            return lastContact;
        }

        /**
         * 当前网络是否处于空闲状态
         */
        private boolean isIdle(){
            return rpcCount == 0;
        }

        /**
         * rpcCount增加
         */
        private void incRpcCount(){
            rpcCount++;
        }

        /**
         * rpcCount减少
         */
        private void decRpcCount(){
            rpcCount--;
        }

        private synchronized void close(){

        }

        public String getHostAddress(){
            return hostAddress;
        }

        int readAndProcess() throws IOException, InterruptedException{
            while (true){
                //至少读一次RPC的数据
                //一直迭代直到一次RPC的数据读完或者没有剩余的数据
                int count = -1;
                /** 读取连接连接时的请求头 */
                if (!connectionHeaderRead){
                    /** 读取myrpc，具体信息参见{@link Client.Connection#writeConnectionHeader(OutputStream)} */
                    if (connectionHeaderHeaderBuf.remaining() > 0){
                        count = channelRead(channel, connectionHeaderHeaderBuf);
                        if (count < 0 || connectionHeaderHeaderBuf.remaining() > 0){
                            //读取有问题或者未读完，直接返回
                            return count;
                        }
                    }
                    /** 读取建立连接时的请求头剩余信息，具体信息参见{@link Client.Connection#writeConnectionHeader(OutputStream)} */
                    if (connectionHeaderBuf == null){
                        connectionHeaderBuf = ByteBuffer.allocate(2);
                    }
                    count = channelRead(channel, connectionHeaderBuf);
                    if (count < 0 || connectionHeaderBuf.remaining() > 0){
                        return count;
                    }
                    /** 这里只取serviceClass，因为AuthProtocol还未使用 */
                    int serviceClass = connectionHeaderBuf.get(0);

                    /** 对建立连接的头信息进行有效性检查 */
                    connectionHeaderHeaderBuf.flip();
                    if (!RPCConstants.HEADER.equals(connectionHeaderHeaderBuf)){
                        LOG.warn("Incorrect header from" +
                                hostAddress + ":" + remotePort);
                        return -1;
                    }

                    connectionHeaderHeaderBuf.clear();
                    connectionHeaderBuf = null;
                    connectionHeaderRead = true;
                    continue;
                }

                /** 读取数据长度 */
                if (dataLengthBuffer.remaining() > 0){
                    count = channelRead(channel, dataLengthBuffer);
                    if (count < 0 || dataLengthBuffer.remaining() > 0){
                        //读取有问题或者未读完
                        return count;
                    }
                }

                /** 读取数据 */
                if (data == null){
                    dataLengthBuffer.flip();
                    int dataLength = dataLengthBuffer.getInt();
                    checkDataLength(dataLength);
                    data = ByteBuffer.allocate(dataLength);
                }

                count = channelRead(channel, data);
                if (data.remaining() == 0){
                    //说明数据已经完全读入
                    dataLengthBuffer.clear();
                    data.flip();
                    /** processOneRpc方法可能改变connectionContextRead值 */
                    boolean isHeaderRead = connectionContextRead;
                    processOneRpc(data.array());
                    data = null;
                    if (!isHeaderRead){
                        continue;
                    }
                }
                return count;
            }
        }

        /**
         * 检验dataLength大小，防止太大导致分配内存过大而影响服务端运行
         *
         * @param dataLength 长度
         * @throws IOException
         */
        private void checkDataLength(int dataLength) throws IOException{
            if (dataLength < 0){
                String errMsg = "Unexpected data length " + dataLength + "! from " + hostAddress;
                LOG.warn(errMsg);
                throw new IOException(errMsg);
            }
            if (dataLength > maxDataLength){
                String errMsg = "Requested data length " + dataLength + "is longer than maximum configured RPC length " + maxDataLength + ". RPC came from " + hostAddress;
                LOG.warn(errMsg);
                throw new IOException(errMsg);
            }
        }

    }

    public static class WrappedRpcServerException extends RpcServerException{
        private RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errorCode;

        public WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errorCode, IOException ioe){
            super(ioe.toString(), ioe);
            this.errorCode = errorCode;
        }

        public WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errorCode, String message){
            this(errorCode, new RpcServerException(message));
        }

        @Override
        public RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto getRpcErrorCodeProto(){
            return errorCode;
        }

        @Override
        public String toString(){
            return getCause().toString();
        }
    }

    private class ConnectionManager{
        /** 当前网络连接的数量 */
        private final AtomicInteger count = new AtomicInteger();
        /** 存放所有的网络连接 */
        private Set<Connection> connections;

        /** 扫描空闲连接的定时器 */
        final private Timer idleScanTimer;
        /** 定时器延迟多久时间后执行，单位：毫秒 */
        final private int idleScanInterval;
        /** 需要关闭空闲连接的阈值，当前连接数超过该阈值便关闭空闲的连接 */
        final private int idleScanThreshold;
        /** 允许网络连接的最大空闲时间，超过该时间有可能被回收 */
        final private int maxIdleTime;
        /** 每一次扫描关闭的最大的连接数量 */
        final private int maxIdleToClose;

        ConnectionManager(){
            this.idleScanTimer = new Timer("IPC Server idle connection scanner for port" + port, true);
            this.idleScanInterval = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
            this.idleScanThreshold = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
            this.maxIdleTime = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.maxIdleToClose = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
            /** 线程安全的 */
            this.connections = Collections.newSetFromMap(
                    new ConcurrentHashMap<Connection, Boolean>(maxQueueSize));
        }

        private boolean add(Connection connection){
            boolean added = connections.add(connection);
            if (added){
                count.getAndIncrement();
            }
            return added;
        }

        private boolean remove(Connection connection){
            boolean removed = connections.remove(connection);
            if (removed){
                count.getAndDecrement();
            }
            return removed;
        }

        /**
         * 获取当前连接数
         *
         * @return 当前连接数
         */
        int size(){
            return count.get();
        }

        /**
         * 为NIO通道创建Connection对象
         *
         * @param channel NIO通道
         * @return 创建的Connection对象
         */
        Connection register(SocketChannel channel){
            Connection connection = new Connection(channel, System.currentTimeMillis());
            add(connection);
            if (LOG.isDebugEnabled()){
                LOG.debug("Server connection from" + connection +
                        "; # active connections:" + size() +
                        "; # queued calls: " + callQueue.size());
            }
            return connection;
        }

        /**
         * 关闭当前连接
         *
         * @param connection 当前连接
         * @return 成功与否
         */
        private boolean close(Connection connection){
            boolean exists = remove(connection);
            if (exists){
                LOG.debug(Thread.currentThread().getName() +
                        ": disconnection client " + connection +
                        ". Number of active connections: " + size());
                connection.close();
            }
            return exists;
        }

        /**
         * 关闭所有连接
         */
        private void closeAll(){
            for (Connection connection : connections){
                close(connection);
            }
        }

        /**
         * 关闭空闲的连接
         *
         * @param scanAll 是否扫描所有的connection
         */
        synchronized void closeIdle(boolean scanAll){
            long minLastContact = System.currentTimeMillis() - maxIdleTime;

            /**
             * 下面迭代的过程中可能遍历不到新插入的Connection对象
             * 但是没有关系，因为新的Connection对象不会是空闲的
             */
            int closed = 0;
            for (Connection connection : connections){
                /**
                 * 不需要扫描全部的情况下，如果没有达到扫描空闲connection的阈值
                 * 或者下面代码关闭连接导致剩余连接小于idleScanThreshold时，便退出循环
                 */
                if (!scanAll && size() < idleScanThreshold){
                    break;
                }

                /**
                 * 关闭空闲的连接，由于Java && 的短路运算，
                 * 如果scanAll == false，最多关闭maxIdleToClose个连接，否则全关闭
                 */
                if (connection.isIdle() &&
                        connection.getLastContact() < minLastContact &&
                        close(connection) &&
                        !scanAll && (++closed==maxIdleToClose)){
                    break;
                }
            }
        }

        /**
         * 定期扫描连接并关闭空闲连接
         * 默认不扫描所有的连接
         */
        private void scheduleIdleScanTask(){
            if (!running){
                return;
            }
            TimerTask idleCloseTask = new TimerTask() {
                @Override
                public void run() {
                    if (!running){
                        return;
                    }
                    if (LOG.isDebugEnabled()){
                        LOG.debug(Thread.currentThread().getName() + ": task running");
                    }

                    try {
                        closeIdle(false);
                    }finally {
                        /** 定时器只调度一次，所以本次任务执行完后手动再次添加到定时器中 */
                        scheduleIdleScanTask();
                    }
                }
            };
            /** 将idleCloseTask任务放入定时器idleScanTimer中 */
            idleScanTimer.schedule(idleCloseTask, idleScanInterval);
        }

        /**
         * 启动定时任务
         */
        void startIdleScan(){
            scheduleIdleScanTask();
        }

        /**
         * 停止定时任务
         */
        void stopIdleScan(){
            idleScanTimer.cancel();
        }
    }

    public static class Call{
        private final int callId;
        private final int retryCount;
        /** 客户端发来的序列化的RPC请求 */
        private final Writable rpcRequest;
        private final Connection connection;
        private long timestamp;

        /** 本次调用的响应信息 */
        private ByteBuffer response;
        private final RPC.RpcKind rpcKind;
        private final byte[] clientId;

        public Call(int id, int retryCount, Writable rpcRequest, Connection connection){
            this(id, retryCount, rpcRequest, connection,
                    RPC.RpcKind.RPC_PROTOCOL_BUFFER, RPCConstants.DUMMY_CLIENT_ID);
        }

        public Call(int id, int retryCount, Writable rpcRequest,
                    Connection connection, RPC.RpcKind rpcKind, byte[] clientId){
            this.callId = id;
            this.retryCount = retryCount;
            this.rpcRequest = rpcRequest;
            this.connection = connection;
            this.timestamp = System.currentTimeMillis();
            this.response = null;
            this.rpcKind = rpcKind;
            this.clientId = clientId;
        }

        public void setResponse(ByteBuffer response){
            this.response = response;
        }

        @Override
        public String toString(){
            return rpcRequest + "from" + connection + "Call#" + callId +
                    "Retry#" + retryCount;
        }
    }

}
