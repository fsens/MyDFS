package org.DFSdemo.ipc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.DataOutputOutputStream;
import org.DFSdemo.io.Writable;
import org.DFSdemo.util.ProtoUtil;

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
        private Invoker(Class<?> protocol,
                        InetSocketAddress address,
                        Configuration configuration,
                        SocketFactory factory,
                        int rpcTimeOut){

        }

        /** 代理对象的方法定义处 */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    interface RpcWrapper extends Writable{
       /** 获取序列化后的长度 */
        int getLength();
    }

    private static abstract class BaseRpcMessageWithHeader<T extends GeneratedMessage> implements RpcWrapper{

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
}
