package org.DFSdemo.ipc;

import com.google.protobuf.*;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.DataOutputOutputStream;
import org.DFSdemo.io.Writable;
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

    /** Invoker作为代理类，代理对象的方法实际上是在这里面定义的 */
    private static class Invoker implements RpcInvocationHandler {

        private Client client;
        private Client.ConnectionID remoteId;
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
            this.remoteId = new Client.ConnectionID(address, protocol, rpcTimeOut, conf);
            this.protocolName = RPC.getProtocolName(protocol);
        }

        /** 代理对象的方法定义处 */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startTime = 0;
            if (LOG.isDebugEnabled()){
                startTime = System.currentTimeMillis();
            }
            return null;
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

        @Override
        public void close() throws IOException {

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
}
