package org.DFSdemo.ipc;

import com.google.protobuf.*;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.DataOutputOutputStream;
import org.DFSdemo.io.Writable;
import org.DFSdemo.ipc.protobuf.RpcHeaderProtos;
import org.DFSdemo.protocol.proto.ProtobufRpcEngineProtos;
import org.DFSdemo.protocol.proto.ProtobufRpcEngineProtos.*;
import org.DFSdemo.util.ProtoUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;


/**
 *实现RpcEngine接口，该类为客户端接口提供代理
 */
public class ProtobufRpcEngine implements RpcEngine{

    //用于打印invoke过程中使用ProtobufRpcEngine的日志
    public static final Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);

    /**
     * 调用registerProtocolEngine完成RpcKind和RpcRequestWrapper的注册
     */
    static {
        org.DFSdemo.ipc.Server.registerProtocolEngine(
                RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                RpcRequestWrapper.class,
                new Server.ProtobufRpcInvoker());
    }

    /** 获取代理对象 */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol,
                          InetSocketAddress address,
                          Configuration conf,
                          SocketFactory factory,
                          int rpcTimeout) throws IOException {
        final Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeout);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(),new Class[]{protocol},invoker);
    }

    @Override
    public RPC.Server getServer(Class<?> protocol, Object instance,
                                String bindAddress, int port,
                                int numHandlers, int numReaders,
                                int queueSizePerHandler, boolean verbose, Configuration conf) throws IOException {
        return new Server(protocol, instance, bindAddress, port,
                numHandlers, numReaders, queueSizePerHandler, verbose, conf);
    }

    /** Invoker作为代理类，代理对象的方法实际上是在这里面定义的 */
    private static class Invoker implements RpcInvocationHandler {

        private Client client;
        private Client.ConnectionId remoteId;
        private final String protocolName;
        private final int NORMAL_ARGS_LEN = 2;

        /**
         * Invoker类的构造方法，初始化Invoker类的私有字段
         *
         * @param protocol 协议的Class
         * @param address 服务端地址
         * @param conf 配置
         * @param factory socket factory
         * @param rpcTimeOut 超时时间
         */
        private Invoker(Class<?> protocol,
                        InetSocketAddress address,
                        Configuration conf,
                        SocketFactory factory,
                        int rpcTimeOut){
            this.client = new Client(RpcResponseWrapper.class, conf, factory);
            this.remoteId = new Client.ConnectionId(address, protocol, rpcTimeOut, conf);
            this.protocolName = RPC.getProtocolName(protocol);
        }

        /**
         * RPC在客户端的invoker
         *
         * 该方法仅抛出ServiceException异常：
         * 将所有的服务调用异常都归纳为一种异常类型ServiceException，可以方便异常处理和日志记录。
         * 以下两种情况都构造ServiceException
         * 1.该方法中客户端抛出异常
         * 2.服务端的异常包装在RemoteException中的异常
         *
         * @param proxy 代理后的对象，一般不使用
         * @param method 调用的方法
         * @param args 调用方法的参数
         * @return Message类型的返回值，包装了远程调用的返回信息（theResponseRead）
         * @throws ServiceException 异常
         */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startTime = 0;
            if (LOG.isDebugEnabled()){
                startTime = System.currentTimeMillis();
            }

            /**
             * 被代理的实际上是PB类，该类将原类的所有方法都转为了两参方法：
             * 1.控制器：一般为null；
             * 2.___proto类封装的参数
             */
            if (args.length != NORMAL_ARGS_LEN){
                throw new ServiceException("Too many parameters for request.Method: ["
                        + method.getName() + "]" + ",Expected 2:Actual: "
                        + args.length);
            }
            if (args[1] == null){
                throw new ServiceException("null param while calling Method: ["
                        + method.getName() + "]");
            }

            //远程调用
            ProtobufRpcEngineProtos.RequestHeaderProto header = constructRpcRequestHeader(method);
            Message theRequest = (Message) args[1];
            final RpcResponseWrapper res;
            try {
                res = (RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                        new RpcRequestWrapper(header, theRequest),
                        remoteId);
            }catch (Throwable e){
                throw new ServiceException(e);
            }

            if (LOG.isDebugEnabled()){
                long callTime = System.currentTimeMillis() - startTime;
                LOG.debug("Call: " + method.getName() + "took " + callTime + " ms");
            }

            //处理远程调用的返回值
            Message protoType = null;
            try {
                //获取返回类型的实例
                protoType = getReturnType(method);
            }catch (Exception e){
                throw new ServiceException(e);
            }
            Message returnMessage = null;
            try {
                //将远程调用返回值中的二进制返回值（即theResponseRead）复制到新的消息对象中
                //其实也就是反序列化了返回值
                returnMessage = protoType.newBuilderForType()
                        .mergeFrom(res.theResponseRead)
                        .build();
            }catch (Throwable t){
                throw new ServiceException(t);
            }

            return returnMessage;
        }

        /**
         * 根据传入方法的Method实例构造该方法的RequestHeaderProto对象
         *
         * @param method 一个方法的Method实例
         * @return 用传入方法的公共字段封装成的RequestHeaderProto对象
         */
        private RequestHeaderProto constructRpcRequestHeader(Method method){
            RequestHeaderProto.Builder headerBuilder = RequestHeaderProto.newBuilder();
            headerBuilder.setMethodName(method.getName());
            headerBuilder.setDeclaringClassProtocolName(protocolName);

            return headerBuilder.build();
        }

        /**
         * 获取返回值类型为Message的实例
         *
         * 由于被代理的类已经是PB类型的，所以请求参数和返回类型都是经过...proto类封装过的了
         *
         * @param method 一个方法实例
         * @return 一个Message实例
         * @throws Exception
         */
        private Message getReturnType(Method method) throws Exception{
            //获取传入方法实例的返回值类型
            Class<?> returnType = method.getReturnType();
            //获取指定为getDefaultInstance的方法实例
            Method newInstMethod = returnType.getMethod("getDefaultInstance");
            //将该方法实例设置为可访问，因为getDefaultInstance可能为私有方法
            newInstMethod.setAccessible(true);
            //反射调用该方法，并将返回类型强制转换为Message
            //第一个参数表示调用方法的所在的对象或类，如果是静态方法，可以设为null
            //第二次参数表示调用方法的参数列表。null前面的(Object[]) 仅仅是为了指明相应参数的类型，可以省略
            return (Message) newInstMethod.invoke(null, (Object[]) null);
        }

        /**
         * 停止client
         *
         * @throws IOException
         */
        @Override
        public void close() throws IOException {
            client.stop();
        }
    }

    interface RpcWrapper extends Writable{
       /** 获取序列化后的长度 */
        int getLength();
    }

    /**
     * 这是一个序列化和反序列化的基类，该类封装了一些基本的属性和序列化/反序列化方法
     * 由它的子类来满足不同场景的需要
     *
     * @param <T> 泛型，并且指定了泛型的类需要是GeneratedMessageV3的子类
     */
    private static abstract class BaseRpcMessageWithHeader<T extends GeneratedMessageV3> implements RpcWrapper{

        T requestHeader;//包含了公共信息，如方法名、接口名等

        /**
         * 用于客户端
         */
        Message theRequest;//请求的参数

        /**
         * 用于服务端
         */
        byte[] theRequestRead;//序列化后的请求参数

        public BaseRpcMessageWithHeader(){};

        public BaseRpcMessageWithHeader(T requestHeader, Message theRequest){
            this.requestHeader = requestHeader;
            this.theRequest = theRequest;
        }

        @Override
        public void write(DataOutput out) throws IOException{
            //由于protobuf提供的writeDelimitedTo方法需要OutputStream类型，所以要将DataOutput类型适配成OutputStream类型
            OutputStream os = DataOutputOutputStream.constructDataOutputStream(out);

            //利用protobuf提供的writeDelimitedTo方法序列化请求
            requestHeader.writeDelimitedTo(os);
            theRequest.writeDelimitedTo(os);
        }

        @Override
        public void readFields(DataInput in) throws IOException{
            requestHeader = parseHeaderFrom(readVarIntBytes(in));
            theRequestRead = readMessageRequest(in);
        }

        /**
         * 对具体的theRequest的反序列化由子类完成
         * 子类会覆写该方法
         *
         * @param in 输入流
         * @return 字节数组
         * @throws IOException
         */
        byte[] readMessageRequest(DataInput in) throws IOException{
            return readVarIntBytes(in);
        }

        /**
         * 将输入流读出为字节数组
         *
         * @param in 输入流
         * @return 字节数组
         * @throws IOException
         */
        private byte[] readVarIntBytes(DataInput in) throws IOException{
            int length = ProtoUtil.readRawVarInt32(in);

            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return bytes;
        }

        /** 由子类完成对具体请求的反序列化 */
        abstract T parseHeaderFrom(byte[] bytes) throws IOException;

        /**
         * 序列化后的长度包括两部分
         * 1.header序列化后的长度以及长度本身的varInt32编码后的长度
         * 2.request序列化后的长度以及长度本身的varInt32编码后的长度
         *
         * @return 序列化后的数据总长度
         */
        @Override
        public int getLength(){
            int headerLen = requestHeader.getSerializedSize();
            int requestLen;
            if (theRequest != null){
                requestLen = theRequest.getSerializedSize();
            } else if (theRequestRead != null) {
                requestLen = theRequestRead.length;
            }else {
                throw new IllegalArgumentException("getLength on uninitialized RpcWrapper");
            }
            return CodedOutputStream.computeUInt32SizeNoTag(headerLen) + headerLen +
                    CodedOutputStream.computeUInt32SizeNoTag(requestLen) + requestLen;
        }
    }

    /**
     * 该类是BaseRpcMessageWithHeader的子类，用来封装请求
     * 该类除了 parseHeaderFrom 方法外，其余都复用基类
     */
    private static class RpcRequestWrapper extends BaseRpcMessageWithHeader<RequestHeaderProto>{
        @SuppressWarnings("unused")
        public RpcRequestWrapper(){};

        public RpcRequestWrapper(RequestHeaderProto requestHeader, Message theRequest){
            super(requestHeader, theRequest);
        }

        /**
         * 在子类中反序列化requestHeader
         *
         * @param bytes
         * @return
         * @throws IOException
         */
        @Override
        RequestHeaderProto parseHeaderFrom(byte[] bytes) throws IOException{
            return RequestHeaderProto.parseFrom(bytes);
        }

        @Override
        public String toString(){
            return requestHeader.getDeclaringClassProtocolName() + "." +
                    requestHeader.getMethodName();
        }
    }

    /**
     * 这是返回值的包装类
     * 其中反序列化由其子类覆写实现
     */
    public static class RpcResponseWrapper implements RpcWrapper{
        Message theResponse;
        byte[] theResponseRead;

        public RpcResponseWrapper(){
        }

        public RpcResponseWrapper(Message theResponse){
            this.theResponse = theResponse;
        }

        @Override
        public void write(DataOutput out) throws IOException{
            OutputStream os = DataOutputOutputStream.constructDataOutputStream(out);

            theResponse.writeDelimitedTo(os);
        }

        @Override
        public void readFields(DataInput in) throws IOException{
            int len = ProtoUtil.readRawVarInt32(in);

            theResponseRead = new byte[len];
            in.readFully(theResponseRead);
        }

        @Override
        public int getLength(){
            int resLen = 0;
            if (theResponse != null){
                resLen = theResponse.getSerializedSize();
            } else if (theResponseRead != null) {
                resLen = theResponseRead.length;
            }else {
                throw new IllegalArgumentException("getLength on uninitialized RpcWrapper");
            }
            return CodedOutputStream.computeUInt32SizeNoTag(resLen) + resLen;
        }
    }

    /**
     * 这是包装建立连接后发送的上下文的类
     */
    public static class RpcRequestMessageWrapper extends
            BaseRpcMessageWithHeader<RpcHeaderProtos.RpcRequestHeaderProto>{

        public RpcRequestMessageWrapper(RpcHeaderProtos.RpcRequestHeaderProto requestHeader, Message theRequest){
            super(requestHeader, theRequest);
        }

        @Override
        RpcHeaderProtos.RpcRequestHeaderProto parseHeaderFrom(byte[] bytes) throws IOException{
            return RpcHeaderProtos.RpcRequestHeaderProto.parseFrom(bytes);
        }
    }

    private static class Server extends RPC.Server{
        /**
         * 构造protocol buffer rpc server
         *
         * @param protocol 接口（协议）
         * @param protocolImpl 接口（协议）的实例
         * @param bindAddress 服务端地址
         * @param port 服务端接口
         * @param numHandlers handler线程个数
         * @param numReaders reader线程个数
         * @param queueSizePerHandler 每Handler期望的消息队列大小
         * @param verbose 是否对调用信息打log
         * @param conf Configuration对象
         * @throws IOException
         */
        public Server(Class<?> protocol, Object protocolImpl,
                      String bindAddress, int port,
                      int numHandlers, int numReaders,
                      int queueSizePerHandler, boolean verbose,
                      Configuration conf) throws IOException{
            super(bindAddress, port, numHandlers, numReaders, queueSizePerHandler, conf);
            this.verbose = verbose;
            registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, protocolImpl);
        }

        static class ProtobufRpcInvoker implements RPC.RpcInvoker{
            @Override
            public Writable call(RPC.Server server, String protocol, Writable rpcRequest, long receiveTime)
                throws Exception{
                /** 根据rpcRequest获取到请求方法、接口（协议）等信息 */
                RpcRequestWrapper request = (RpcRequestWrapper) rpcRequest;
                RequestHeaderProto requestHeader = request.requestHeader;
                String methodName = requestHeader.getMethodName();
                String protoName = requestHeader.getDeclaringClassProtocolName();

                if (server.verbose){
                    LOG.info("Call: protocol=" + protocol + ",method=" + methodName);
                }

                /** 获取缓存的接口实现类对象 */
                ProtoClassProtoImpl protoClassProtoImpl = RPC.getProtocolImp(RPC.RpcKind.RPC_PROTOCOL_BUFFER, server, protoName);

                /** 通过BlockingService获取到调用方法的methodDescriptor */
                BlockingService service = (BlockingService) protoClassProtoImpl.protocolImpl;
                Descriptors.MethodDescriptor methodDescriptor = service.getDescriptorForType().findMethodByName(methodName);
                if (methodDescriptor == null){
                    String msg = "Unknown method" + methodName + "called on" + protocol + "protocol.";
                    LOG.warn(msg);
                    throw new RpcNoSuchMethodException(msg);
                }
                /** 通过BlockingService和methodDescriptor获取到方法参数的类对象protoType */
                Message protoType = service.getRequestPrototype(methodDescriptor);
                /** 给方法参数的类对象protoType赋值，其实就是反序列化请求参数 */
                Message param = protoType.newBuilderForType()
                        .mergeFrom(request.theRequestRead)
                        .build();

                Message result;

                long startTime = System.currentTimeMillis();
                //从接收请求到调用前的时间
                int qTime = (int) (startTime - receiveTime);

                Exception exception = null;

                try {
                    /**
                     * 根据BlockingService的callBlockingMethod方法调用请求的方法，最终会执行
                     * ClientNamenodeProtocolServerSideTranslatorPB中的方法
                     */
                    result = service.callBlockingMethod(methodDescriptor, null, param);
                }catch (ServiceException se){
                    //获取根本原因
                    exception = (Exception) se.getCause();
                    throw exception;
                }catch (Exception e){
                    exception = e;
                    throw exception;
                }finally {
                    //处理请求的时间
                    long processTime = (int) (System.currentTimeMillis() - startTime);
                    if (LOG.isDebugEnabled()){
                        String msg = "Served:" + methodName + "queueTime= " + qTime +
                                "processingTime= " + processTime;
                        if (exception != null){
                            msg += "exception= " + exception.getClass().getSimpleName();
                        }
                        LOG.debug(msg);
                    }
                }
                /** 将返回结果构造为RpcResponseWrapper对象，以便后续能够序列化发送给客户端 */
                return new RpcResponseWrapper(result);
            }
        }
    }

}
