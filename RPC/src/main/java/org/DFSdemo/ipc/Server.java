package org.DFSdemo.ipc;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.DFSdemo.conf.CommonConfigurationKeysPublic;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.Writable;
import org.DFSdemo.ipc.protobuf.IpcConnectionContextProtos;
import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;
import org.DFSdemo.protocol.RPCConstants;
import org.DFSdemo.util.ProtoUtil;
import org.DFSdemo.util.ReflectionUtils;
import org.DFSdemo.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
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

    private void closeConnection(Connection conn){
        connectionManager.close(conn);
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
     * 对远程调用请求的实现，其实是调用诸如ProtobufRpcInvoker的call方法实现的
     * 这个让子类{@link RPC.Server}实现
     *
     * @param rpcKind 序列化类型
     * @param protocol 协议
     * @param param 客户端请求
     * @param receiveTime 接收客户端调用的请求的时间
     * @return 返回值的protobuf类对象
     * @throws Exception
     */
    public abstract Writable call(RPC.RpcKind rpcKind, String protocol, Writable param, long receiveTime) throws Exception;

    /**
     * 从rpcKindMap中获取对应序列化的rpcInvoker对象
     *
     * @param rpcKind 序列化类型
     * @return 传入序列化类型对应的rpcInvoker对象
     */
    public static RPC.RpcInvoker getRpcInvoker(RPC.RpcKind rpcKind){
        RpcKindMapValue val = rpcKindMap.get(rpcKind);
        return (val == null) ? null : val.rpcInvoker;
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
                Reader reader = new Reader("Socket Reader #" + (i + 1) + " for port " + port);
                this.readers[i] = reader;
                reader.start();
            }

            this.setName("IPC Server listener on " + port);
            /** 设置为daemon，它的线程也是daemon */
            this.setDaemon(true);
        }

        @Override
        public void run(){
            LOG.info("Listener thread " + Thread.currentThread().getName() + " : starting");
            connectionManager.startIdleScan();
            while (running){
                SelectionKey key = null;
                try {
                    /** 阻塞等待，直到一下其中一个发生1.至少有一个通道准备好I/O操作或者2.被中断 */
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

                //由于有多个Reader线程，所以每次accept新的连接时得选一个放入
                Reader reader = getReader();
                //放入选定的Reader线程的pendingConnections中
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
                            //take会移除队列中的头元素，也就是说，Reader线程只会对新的连接注册OP_READ
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
            this.setName("IPC Server handler " + instanceNumber + "on " + port);
        }

        @Override
        public void run(){
            LOG.debug(Thread.currentThread().getName() + ": starting.");
            ByteArrayOutputStream buf = new ByteArrayOutputStream(10240);
            while (running){
                try {
                    /** 线程安全的队列 */
                    final Call call = callQueue.take();
                    if (LOG.isDebugEnabled()){
                        LOG.debug(Thread.currentThread().getName() + " : " + call +
                                " for rpcKind " + call.rpcKind);
                    }
                    if (!call.connection.channel.isOpen()){
                        LOG.info(Thread.currentThread().getName() + " : skipped " + call);
                        continue;
                    }
                    /** 异常类 */
                    String errorClass = null;
                    /** 发生异常的堆栈信息 */
                    String error = null;
                    /** 返回状态 */
                    RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto returnStatus = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS;
                    /** 错误类型 */
                    RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto detailedErr = null;
                    Writable value = null;

                    try {
                        value = call(call.rpcKind, call.connection.protocolName, call.rpcRequest, call.timestamp);
                    }catch (Throwable e){
                        String logMsg = Thread.currentThread().getName() + ",call " + call;
                        if (e instanceof RuntimeException || e instanceof Error){
                            /** 抛出该类型的错误说明服务端自身出现问题 */
                            LOG.warn(logMsg, e);
                        }else {
                            /** 属于正常情况的异常抛出 */
                            LOG.info(logMsg, e);
                        }
                        if (e instanceof RpcServerException){
                            RpcServerException rse = (RpcServerException) e;
                            returnStatus = ((RpcServerException) e).getRpcStatusProto();
                            detailedErr = rse.getRpcErrorCodeProto();
                        }else {
                            returnStatus = RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR;
                            detailedErr = RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_APPLICATION;
                        }
                        errorClass = e.getClass().getName();
                        error = StringUtils.stringifyException(e);
                    }
                    setupResponse(buf, call, returnStatus, detailedErr, value, errorClass, error);
                    /** 如果buf占用空间太大则丢弃，重新将buf调到初始大小以释放内存。则是由于可能会存在大部分响应很小，其中一个响应很大的情况 */
                    if (buf.size() > maxRespSize){
                        LOG.info("Large response size " + buf.size() + " for call "
                        + call.toString());
                        buf = new ByteArrayOutputStream(10240);
                    }
                    responder.doResponse(call);
                }catch (InterruptedException e){
                    LOG.info(Thread.currentThread().getName() + "unexpectedly interrupted", e);
                }catch (Exception e){
                    LOG.info(Thread.currentThread().getName() + "caught an exception", e);
                }
            }
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread{

        private final Selector writeSelector;
        private int pending;

        final static int PURGE_INTERVAL = 90000;//15min

        Responder() throws IOException{
            this.setName("IPC Server Responder");
            this.setDaemon(true);
            writeSelector = Selector.open();
            pending = 0;
        }

        @Override
        public void run(){
            LOG.info(Thread.currentThread().getName() + ": starting.");
            try {
                doRunLoop();
            }finally {
                LOG.info("Stopping " + Thread.currentThread().getName());
                try {
                    writeSelector.close();
                }catch (IOException ioe){
                    LOG.error("Couldn't close write selector in " + Thread.currentThread().getName(), ioe);
                }
            }
        }

        private void doRunLoop(){
            long lastPurgeTime = 0;
            while (running){
                try {
                    /** 如果有管道正在注册，则等待 */
                    waitPending();
                    writeSelector.select(PURGE_INTERVAL);
                    Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
                    while (iter.hasNext()){
                        SelectionKey key = iter.next();
                        iter.remove();
                        try {
                            if (key.isValid() && key.isWritable()){
                                doAsyncWrite(key);
                            }
                        }catch (IOException ioe){
                            LOG.info(Thread.currentThread().getName() + ": doAsyncWrite threw exception " + ioe);
                        }
                    }

                    long now = System.currentTimeMillis();
                    if (now < lastPurgeTime + PURGE_INTERVAL){
                        continue;
                    }
                    lastPurgeTime = now;

                    if (LOG.isDebugEnabled()){
                        LOG.debug("Checking for call responses.");
                    }

                    ArrayList<Call> calls = null;
                    /** 收集所有等待被发送的响应Call对象 */
                    synchronized (writeSelector.keys()){
                        /** 锁住writeSelector.keys()对象，防止新的channel注册 */
                        calls = new ArrayList<>(writeSelector.keys().size());
                        iter = writeSelector.keys().iterator();
                        while (iter.hasNext()){
                            SelectionKey key = iter.next();
                            Call call = (Call) key.attachment();
                            if (call != null && key.channel() == call.connection.channel){
                                calls.add(call);
                            }
                        }
                    }

                    /** 如果有长时间（超过PURGE_INTERVAL）未发送的calls，关闭连接，丢弃它们 */
                    for (Call call : calls){
                        doPurge(call, now);
                    }
                }catch (OutOfMemoryError e){
                    //如果太多事件需要响应，可能会内存溢出
                    LOG.warn("Out of Memory in server select" ,e);
                    try{Thread.sleep(60000);} catch (Exception ie){}
                }catch (Exception e){
                    LOG.warn("Exception in Responder", e);
                }
            }

        }

        /**
         * 清理传入call所在连接的responseQueue队列
         * 如果存在超过PURGE_INTERVAL还未响应的call，说明它所在的连接很可能有问题，则关闭它所在的连接
         *
         * @param call
         * @param now
         */
        private void doPurge(Call call, long now){
            LinkedList<Call> responseQueue = call.connection.responseQueue;
            synchronized (responseQueue){
                ListIterator<Call> iter = responseQueue.listIterator(0);
                while (iter.hasNext()){
                    call = iter.next();
                    if (now > call.timestamp + PURGE_INTERVAL){
                        closeConnection(call.connection);
                        break;
                    }
                }
            }
        }

        void doResponse(Call call) throws IOException{
            synchronized (call.connection.responseQueue){
                call.connection.responseQueue.addLast(call);
                /**
                 * 如果队列只有当前的响应信息，直接写响应信息
                 * 否则，只写入队列，Handler线程不做响应信息的发送
                 *
                 * 由于是先入队列，所以这里判断是否等于1
                 */
                if (call.connection.responseQueue.size() == 1){
                    processResponse(call.connection.responseQueue, true);
                }
            }
        }

        private void doAsyncWrite(SelectionKey key) throws IOException{
            Call call = (Call) key.attachment();
            if (call == null){
                return;
            }
            if (call.connection.channel != key.channel()){
                throw new IOException("doAsyncWrite: bad channel");
            }
            synchronized (call.connection.responseQueue){
                if (processResponse(call.connection.responseQueue, false)){
                    try {
                        /** 取消通道上的注册事件 */
                        key.interestOps(0);
                    }catch (CancelledKeyException e){
                        LOG.warn("Exception while changing ops : " + e);
                    }
                }
            }
        }

        /**
         *  doResponse 和 doAsyncWrite的公共方法
         *
         * @param responseQueue Call对象队列
         * @param isHandler 是否是Handler或者Reader线程发送响应的标志：是则true，反之则false
         * @return 如果队列中响应已经发送完后者无需再发送响应（错误的情况），则返回ture；队列响应未发完则为false
         * @throws IOException
         */
        private boolean processResponse(LinkedList<Call> responseQueue, boolean isHandler) throws IOException{

            /** error默认为true，后面只要不修改为false，就说明出错了，要进行错误处理 */
            boolean error = true;
            boolean done = false;
            int numElements = 0;
            Call call = null;

            try {
                numElements = responseQueue.size();
                if (numElements == 0){
                    error = false;
                    return true;
                }
                /** 取出第一个Call对象，发送响应信息 */
                call = responseQueue.removeFirst();
                SocketChannel channel = call.connection.channel;
                if (LOG.isDebugEnabled()){
                    LOG.debug(Thread.currentThread().getName() + ": responding to " + call);
                }
                /** 将响应信息发送到channel */
                int numBytes = channelWrite(channel, call.response);
                if (numBytes < 0){
                    return true;
                }

                /** 如果响应一次性发送完 */
                if (!call.response.hasRemaining()){
                    call.response = null;
                    call.connection.decRpcCount();
                    /** 由于responseQueue.removeFirst()并不会改变numElements，所以当numElements为1时就说明已经处理完call队列中的call了 */
                    if (numElements == 1){
                        done = true;
                    }else {
                        done = false;
                    }
                    if (LOG.isDebugEnabled()){
                        LOG.debug(Thread.currentThread().getName() + " responding to " + call
                        + " Wrote " + numBytes + " bytes.");
                    }
                }else {
                    /** 说明没有一次性读取完，继续放入队列中 */
                    call.connection.responseQueue.addFirst(call);

                    /**
                     * 如果isHandler = true，说明该队列只有当前处理的一个Call对象
                     * 如果本次未处理完，需要重新放入队列给该渠道注册一个OP_WRITE事件，后续由Responder线程继续处理
                     */
                    if (isHandler){
                        call.timestamp = System.currentTimeMillis();

                        /** 防止wakeup后register前再次进入select等待，需要加锁，让responder线程等待 */
                        incPending();
                        try {
                            /** 如果writeSelector在select上阻塞，无法成功地register */
                            writeSelector.wakeup();
                            /** 将该call的connection的channel注册到writeSelector上，注册OP_WRITE事件，并在key上attach该call对象 */
                            channel.register(writeSelector, SelectionKey.OP_WRITE, call);
                        }catch (ClosedChannelException e){
                            done = true;
                        }finally {
                            decPending();
                        }
                    }
                    if (LOG.isDebugEnabled()){
                        LOG.debug(Thread.currentThread().getName() + " responding to " + call
                                + " Wrote partial " + numBytes + " bytes.");
                    }
                }
                error = false;
            }finally {
                if (error && call != null){
                    LOG.warn(Thread.currentThread().getName() + ",call " + call + ": output error");
                    done = true;
                    /** 关闭连接 */
                    closeConnection(call.connection);
                }
            }
            return done;
        }

        private synchronized void incPending(){
            pending++;
        }

        private synchronized void decPending(){
            pending--;
            notify();
        }

        private synchronized void waitPending() throws InterruptedException{
            while (pending > 0){
                wait();
            }
        }

    }

    /**
     * 构造服务端响应信息，并赋值给Call对象
     *
     * @param responseBuf 序列化响应信息的buffer
     * @param call {@link Call}对象
     * @param status 调用状态
     * @param errorCode 调用错误码
     * @param resValue 如果调用成功，代表调用的返回值。如果调用失败则为null
     * @param errorClass error class
     * @param error 调用错误的堆栈信息
     */
    private void setupResponse(ByteArrayOutputStream responseBuf,
                               Call call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto status, RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto errorCode,
                               Writable resValue, String errorClass, String error) throws IOException{
        //先清空缓冲区中的数据
        responseBuf.reset();
        DataOutputStream out = new DataOutputStream(responseBuf);
        RpcHeaderProtos.RpcResponseHeaderProto.Builder headerBuilder = RpcHeaderProtos.RpcResponseHeaderProto.newBuilder();
        headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
        headerBuilder.setCallId(call.callId);
        headerBuilder.setRetryCount(call.retryCount);
        headerBuilder.setStatus(status);

        /** 如果客户端请求的调用成功完成 */
        if (status == RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.SUCCESS){
            RpcHeaderProtos.RpcResponseHeaderProto header = headerBuilder.build();
            final int headerLen = header.getSerializedSize();
            int fullLength = CodedOutputStream.computeUInt32SizeNoTag(headerLen) + headerLen;

            try {
                ByteArrayOutputStream bo = new ByteArrayOutputStream();
                DataOutputStream tmpOut = new DataOutputStream(bo);
                /** 先将返回值写入临时输出流中(注意，这里是按照protobuf的方式写入的输出流) */
                resValue.write(tmpOut);
                byte[] data = bo.toByteArray();

                fullLength += data.length;
                /** 写入响应头和返回值的总长度 */
                out.write(fullLength);
                /** 再按照protobuf的方式将响应头序列化写入到输出流中 */
                header.writeDelimitedTo(out);
                /** 最后将返回值按照protobuf的方式序列化写入输出流 */
                out.write(data);
            }catch (Throwable t){
                LOG.warn("Error serializing call response for call " + call, t);
                /** 序列化出错时，需要递归调用该方法，创建一个序列化错误的响应信息 */
                setupResponse(responseBuf, call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.ERROR,
                        RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.ERROR_SERIALIZING,
                        null, t.getClass().getName(), StringUtils.stringifyException(t));
            }
        }else {
            /** 如果客户端调用的请求完成失败，则设置失败信息，就不返回返回值了 */
            headerBuilder.setErrorDetail(errorCode);
            headerBuilder.setExceptionClassName(errorClass);
            headerBuilder.setErrorMsg(error);
            RpcHeaderProtos.RpcResponseHeaderProto header = headerBuilder.build();
            int headerLen = header.getSerializedSize();
            final int fullLength = CodedOutputStream.computeUInt32SizeNoTag(headerLen) + headerLen;
            out.write(fullLength);
            header.writeDelimitedTo(out);
        }
        /** 从输出流中获取序列化的字节数组，并设置到call中的response属性中 */
        call.setResponse(ByteBuffer.wrap(responseBuf.toByteArray()));
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
        private ByteBuffer connectionHeaderHeaderBuf;
        /** 建立连接后发送的头信息中的剩下的 */
        private ByteBuffer connectionHeaderBuf;
        /** 四字节长度 */
        private ByteBuffer dataLengthBuffer;
        /** 存放数据的缓冲区 */
        private ByteBuffer data;
        /** 连接头是否被读过 */
        private boolean connectionHeaderRead = false;
        /** 连接头后的连接上下文是否被读过 */
        private boolean connectionContextRead = false;
        /** 连接上下文对象 */
        IpcConnectionContextProtos.IpcConnectionContextProto connectionContext;
        /** 连接上下文对象中的协议名属性 */
        String protocolName;
        /** 存放响应的call队列 */
        private final LinkedList<Call> responseQueue;

        Connection(SocketChannel channel, long lastContact){
            this.channel = channel;
            this.lastContact = lastContact;
            this.socket = channel.socket();
            this.remoteAddr = socket.getInetAddress();
            this.hostAddress = remoteAddr.getHostAddress();
            this.remotePort = socket.getPort();
            this.connectionHeaderHeaderBuf = ByteBuffer.allocate(5);
            this.dataLengthBuffer = ByteBuffer.allocate(4);
            this.responseQueue = new LinkedList<Call>();
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
                        LOG.warn("Incorrect header from " +
                                hostAddress + " : " + remotePort);
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
                    /** 解析一次RPC：这一次RPC可能是连接上下文，也可能是一个客户端请求调用 */
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

        /**
         * 从输入流中反序列化protobuf对象
         *
         * @param builder 反序列化的目标protobuf类的构造器类
         * @param dis 需要反序列化的输入流
         * @return 反序列化后的protobuf类
         * @param <T>
         * @throws WrappedRpcServerException
         */
        @SuppressWarnings("unchecked")
        private <T extends Message> T decodeProtobufFromStream(Message.Builder builder, DataInputStream dis) throws WrappedRpcServerException{
            try {
                builder.mergeDelimitedFrom(dis);
                return (T) builder.build();
            }catch (Exception e){
                Class<?> protoClass = builder.getDefaultInstanceForType().getClass();
                throw new WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
                        "Error decoding " + protoClass.getSimpleName() + ": " + e);
            }
        }

        /**
         * 处理一次RPC请求
         *
         * @param buf RPC请求对应的字节数组
         * @throws IOException
         * @throws InterruptedException
         */
        private void processOneRpc(byte[] buf) throws IOException, InterruptedException{
            int callId = -1;
            int retry = RPCConstants.INVALID_RETRY_COUNT;

            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buf));
                RpcHeaderProtos.RpcRequestHeaderProto header = decodeProtobufFromStream(RpcHeaderProtos.RpcRequestHeaderProto.newBuilder(), dis);
                callId = header.getCallId();
                retry = header.getRetryCount();
                if (LOG.isDebugEnabled()){
                    LOG.debug("get #" + callId);
                }
                checkRpcHeader(header);
                if (callId < 0){
                    processOutOfBandRequest(header, dis);
                }else if (!connectionContextRead){
                    throw new WrappedRpcServerException(
                            RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                            "Connection context not found");
                }else {
                    processRpcRequest(header, dis);
                }
            }catch (WrappedRpcServerException wrse){
                Throwable ioe = wrse.getCause();
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                Call call = new Call(callId, retry, null, this);
                setupResponse(buffer, call, RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto.FATAL,wrse.getRpcErrorCodeProto(),
                        null, ioe.getClass().getName(), ioe.getMessage());
                responder.doResponse(call);
                throw wrse;
            }
        }

        /**
         * 验证rpc header是否正确
         *
         * @param header RPC request header
         * @throws WrappedRpcServerException header包含无效值
         */
        private void checkRpcHeader(RpcHeaderProtos.RpcRequestHeaderProto header) throws WrappedRpcServerException{
            if (!header.hasRpcKind()){
                String errMsg = "IPC Server: No rpc kind in rpcRequestHeader";
                throw new WrappedRpcServerException(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, errMsg);
            }
        }

        /**
         * 解析客户端调用请求
         *
         * @param header RpcRequestHeaderProto对象，提供了一些头信息
         * @param dis 客户端请求数据输入流
         * @throws WrappedRpcServerException
         * @throws InterruptedException
         */
        private void processRpcRequest(RpcHeaderProtos.RpcRequestHeaderProto header, DataInputStream dis) throws WrappedRpcServerException, InterruptedException{
            /** 根据RPC类型获得调用请求类的封装类 */
            Class<? extends Writable> rpcRequestClass = getRpcRequestWrapper(header.getRpcKind());
            if (rpcRequestClass == null){
                LOG.warn("Unknown RPC kind" + header.getRpcKind() +
                        " from client " + hostAddress);
                final String err = "Unknown rpc kind in rpc header " + header.getRpcKind();
                throw new WrappedRpcServerException(
                        RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        err);
            }
            Writable rpcRequest;
            try {
                /** 实例化对象rpcRequest */
                rpcRequest = ReflectionUtils.newInstance(rpcRequestClass);
                /** 反序列化rpcRequest对象 */
                rpcRequest.readFields(dis);
            }catch (Exception e){
                LOG.warn("Unable to read call parameters for client " +
                        hostAddress + "on connection protocol " +
                        this.protocolName + "for rpcKind " + header.getRpcKind(), e);
                String err = "IPC Server unable to read call parameters: " + e.getMessage();
                throw new WrappedRpcServerException(
                        RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
            }
            /** 构造RPC调用单元的Call对象，将其写入callQueue队列中 */
            Call call = new Call(header.getCallId() , header.getRetryCount(), rpcRequest, this, ProtoUtil.convertRpcKind(header.getRpcKind()), header.getClientId().toByteArray());
            /** 将call入队，有可能在这里阻塞 */
            callQueue.put(call);
            incRpcCount();
        }

        Class<? extends Writable> getRpcRequestWrapper(RpcHeaderProtos.RpcKindPtoto rpcKind){
            RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convertRpcKind(rpcKind));
            return (val == null) ? null : val.rpcRequestWrapperClass;
        }

        /**
         * 处理out of band数据
         *
         * 这里不直接处理连接上下文的原因是，以后可能会添加其他的out of band数据
         *
         * @param header RPC header
         * @param dis 请求的数据流
         * @throws WrappedRpcServerException 状态错误
         */
        private void processOutOfBandRequest(RpcHeaderProtos.RpcRequestHeaderProto header,
                                             DataInputStream dis) throws WrappedRpcServerException{
            int callId = header.getCallId();
            /** 根据callId判断是否是连接上下文 */
            if (callId == RPCConstants.CONNECTION_CONTEXT_CALL_ID){
                processConnectionContext(dis);
            }else {
                throw new WrappedRpcServerException(
                        RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Unknown out of band call #" + callId);

            }
        }

        /**
         * 读取连接上下文（connection context）信息
         *
         * @param dis 客户端请求的数据输入流
         * @throws WrappedRpcServerException 状态错误
         */
        private void processConnectionContext(DataInputStream dis) throws WrappedRpcServerException{
            if (connectionContextRead){
                throw new WrappedRpcServerException(
                        RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Connection context already processed");
            }
            connectionContext = decodeProtobufFromStream(IpcConnectionContextProtos.IpcConnectionContextProto.newBuilder(), dis);
            protocolName = connectionContext.getProtocol();
            connectionContextRead = true;
        }


        @Override
        public String toString(){
            return getHostAddress() + ":" + remotePort;
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
            this.idleScanTimer = new Timer("IPC Server idle connection scanner for port " + port, true);
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
