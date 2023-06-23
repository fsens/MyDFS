package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;
import org.DFSdemo.io.Writable;
import org.DFSdemo.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 该类是一个工具类：
 * 1.该类定义了一些公共的工具方法，方便客户端获取到代理对象
 * 2.该类定义了枚举类来标识不同的序列化方法
 */
public class RPC {

    public static final String RPC_ENGINE = "rpc.engine";

    //默认的rpc服务类，先定义在这里，以后方便扩展
    final static int RPC_SERVICE_CLASS_DEFAULT = 0;

    static final Log LOG = LogFactory.getLog(RPC.class);


    /**
     * 停止代理，该代理需要实现{@link Closeable} 或者 {@link RpcInvocationHandler}
     *
     * @param proxy 需要停止的代理
     *
     * @throws IllegalArgumentException 代理没有实现{@link Closeable} 接口
     */
    public static void stopProxy(Object proxy){
        if (proxy == null){
            throw new IllegalArgumentException("Cannot close proxy since it is null");
        }

        try {
            //判断代理对象是否是Closeable或者其子类的实例
            if (proxy instanceof Closeable){
                ((Closeable) proxy).close();
                return;
            }
        }catch (IOException e){
            LOG.error("Closing proxy or invocation handler causer exception", e);
        }catch (IllegalArgumentException e){
            LOG.error("RPC.stopProxy called on non proxy: class=" + proxy.getClass().getName(), e);
        }

        //proxy没有close方法
        throw new IllegalArgumentException(
                "Cannot close proxy - is not Closeable or "
                + "dose not provide Closeable invocation handler"
                + proxy.getClass()
        );
    }


    /**
     * 为协议设置RPC引擎
     * @param conf 配置
     * @param protocol 协议接口
     * @param engine 实现的引擎
     */
    public static void setProtocolEngine(Configuration conf,Class<?> protocol, Class<?> engine){
        conf.setClass(RPC_ENGINE+"."+protocol.getName(), engine, RpcEngine.class);
    }

    /**
     * 接口与RPC引擎对应关系的缓存
     */
    private static final Map<Class<?>, RpcEngine> PROTOCOL_ENGINES = new HashMap<Class<?>, RpcEngine>();

    /**
     * 根据协议和配置获取该协议对应的RPC引擎
     * @param protocol 协议接口
     * @param conf 配置
     * @return 传入协议对应的RPC引擎
     * @param <T> 表明该方法是个与类无关的泛型方法
     */
    static synchronized <T> RpcEngine getProtocolEngine(Class<T> protocol, Configuration conf){
        RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
        if (engine == null){
            //默认使用ProtobufRpcEngine
            Class<?> clazz = conf.getClass(RPC_ENGINE + "." + protocol.getName(), ProtobufRpcEngine.class);
        try {
            //通过反射实例化RpcEngine的实现类
            engine = (RpcEngine) ReflectionUtils.newInstance(clazz);
            PROTOCOL_ENGINES.put(protocol, engine);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        }
        return engine;
    }

    /**
     * 获取对应协议的名字，优先获得注解中的协议名
     *
     * @param protocol
     * @return
     */
    public static String getProtocolName(Class<?> protocol){
        if (protocol == null){
            return null;
        }

        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        return anno == null ? protocol.getName() : anno.protocolName();
    }

    /**
     * 获取指定协议的代理对象
     * @param protocol 协议接口
     * @param address 服务端地址
     * @param conf 配置
     * @param factory 创建socket的工厂
     * @param rpcTimeout rpc超时时间
     * @param <T> 表明该方法是个与类无关的泛型方法
     * @return 传入协议的代理对象
     * @throws IOException
     */
    public static <T> T getProtocolProxy(Class<T> protocol,
                                         InetSocketAddress address,
                                         Configuration conf,
                                         SocketFactory factory,
                                         int rpcTimeout)
            throws IOException {
        return getProtocolEngine(protocol, conf).getProxy(protocol, address, conf, factory, rpcTimeout);
    }

    /**
     * 定义一各枚举类来标识所用的序列化类型
     * 定义该类是为了方便扩展
     */
    public enum RpcKind{
        /**
         * RPC_BUILTIN 默认值
         * RPC_PROTOCOL_BUFFER ProtobufRpcEngine
         */
        RPC_BUILTIN ((short) 1),
        RPC_PROTOCOL_BUFFER ((short) 2);

        final static int MAX_INDEX = RPC_PROTOCOL_BUFFER.value;
        public final short value;

        RpcKind(short value){
            this.value = value;
        }
    }

    /**
     * RPC Server
     */
    public abstract static class Server extends org.DFSdemo.ipc.Server{
        /** 控制打印日志的开关，该值由下游Server决定 */
        boolean verbose;

        /**
         * 由于父类是抽象类，它的构造方法不能被继承，所以子类得实现自己的构造方法，即使和父类的一样
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
            //调用父类的构造方法来实例化父类的成员变量
            super(bindAddress, port, numHandlers, numReaders, queueSizePerHandler, conf);
        }

        /**
         * 存储协议（接口）的名称（还可以拓展存储其他的信息，如版本等），用来当作map的key
         */
        static class ProtoName{
            final String protoName;

            ProtoName(String protoName){
                this.protoName = protoName;
            }

            @Override
            public boolean equals(Object obj){
                if (obj == null){
                    return false;
                }
                if (this == obj){
                    return true;
                }
                if (! (obj instanceof ProtoName)){
                    return false;
                }
                ProtoName pn = (ProtoName) obj;
                return (this.protoName.equals(pn.protoName));
            }

            @Override
            public int hashCode(){
                //随便乘一个质数
                return protoName.hashCode() * 37;
            }
        }

        /**
         * 存储协议（接口）的Class对象及其实现实例，作为value
         */
        static class ProtoClassProtoImpl{
            Class<?> protocolClass;
            Object protocolImpl;

            ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl){
                this.protocolClass = protocolClass;
                this.protocolImpl = protocolImpl;
            }
        }

        /** 存储server不同序列化方式对应的协议map */
        ArrayList<Map<ProtoName, ProtoClassProtoImpl>> protocolImplMapArray =
                new ArrayList<>(RpcKind.MAX_INDEX);

        /**
         *获得对应RPC序列化方式的HashMap对象
         *
         * @param rpcKind RPC.RpcKind
         * @return RpcKind对应的HashMap对象
         */
        Map<ProtoName, ProtoClassProtoImpl> getProtocolImplMap(RpcKind rpcKind){
            /** 懒加载--直到要用到的时候再加载 */
            if (protocolImplMapArray.size() == 0){
                /** 初始化，为每一种序列化方式添加一个HashMap对象 */
                for (int i = 0; i < RpcKind.MAX_INDEX ; i++){
                    /** 设定HashMap初始化容量为10个key--var。初始化空间设置稍大一点，避免使用的时候容量不足而频繁扩容，也不至于浪费过多空间 */
                    protocolImplMapArray.add(new HashMap<ProtoName, ProtoClassProtoImpl>(10));
                }
            }

            return protocolImplMapArray.get(rpcKind.ordinal());
        }

        /**
         * 注册接口及其实例的键值对
         *
         * @param rpcKind rpc类型
         * @param protocolClass 接口（协议）
         * @param protocolImpl 接口（协议）的实现实例
         */
        void registerProtocolAndImpl(RpcKind rpcKind, Class<?> protocolClass, Object protocolImpl){
            String protocolName = RPC.getProtocolName(protocolClass);

            getProtocolImplMap(rpcKind).put(new ProtoName(protocolName), new ProtoClassProtoImpl(protocolClass, protocolImpl));
            LOG.debug("RpcKind = " + rpcKind + "Protocol Name = " + protocolName +
                    "ProtocolImpl=" + protocolImpl.getClass().getName() +
                    "protocolClass=" + protocolClass.getName());
        }

        /**
         * 提供给上游调用
         * 向server中增加接口（协议）及其实例的键值对
         *
         * @param rpcKind rpc类型
         * @param protocolClass 接口（协议）
         * @param protocolImpl 接口（协议）的实现实例
         */
        public void addProtocol(RpcKind rpcKind, Class<?> protocolClass, Object protocolImpl){
            registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
        }
    }

    /**
     * 该类用于构造RPC Server
     */
    public static class Builder{

        private Class<?> protocol;
        private Object instance;
        private String bindAddress = "0.0.0.0";
        private int bindPort = 0;
        private int numHandlers = 1;
        private int numReaders = -1;
        private boolean verbose = false;
        private int queueSizePerHandler = -1;
        private Configuration conf;

        public Builder(Configuration conf){
            this.conf = conf;
        }

        public Builder setProtocol(Class<?> protocol){
            this.protocol = protocol;
            return this;
        }

        public Builder setInstance(Object instance){
            this.instance = instance;
            return this;
        }

        public Builder setBindAddress(String bindAddress){
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder setBindPort(int bindPort){
            this.bindPort = bindPort;
            return this;
        }

        public Builder setNumHandlers(int numHandlers){
            this.numHandlers = numHandlers;
            return this;
        }

        public Builder setNumReaders(int numReaders){
            this.numReaders = numReaders;
            return this;
        }

        public Builder setVerbose(boolean verbose){
            this.verbose = verbose;
            return this;
        }

        public Builder setQueueSizePerHandler(int queueSizePerHandler){
            this.queueSizePerHandler = queueSizePerHandler;
            return this;
        }

        public Builder setConf(Configuration conf){
            this.conf = conf;
            return this;
        }

        /**
         * 先根据protocol获取对应的RpcEngine，再获取相应RpcEngine的Server实例
         *
         * @return 相应RpcEngine的Server
         * @throws IOException 发生错误
         * @throws IllegalArgumentException 没有设置必要的参数
         */
        public Server build() throws IOException, IllegalArgumentException{
            if (this.conf == null){
                throw new IllegalArgumentException("conf is not set");
            }
            if (this.protocol == null){
                throw new IllegalArgumentException("protocol is not set");
            }
            if (this.instance == null){
                throw new IllegalArgumentException("instance is not set");
            }
            return getProtocolEngine(protocol, conf)
                    .getServer(this.protocol, this.instance, this.bindAddress, this.bindPort,
                            this.numHandlers, this.numReaders, this.queueSizePerHandler,
                            this.verbose, this.conf);
        }
    }

    /**
     * 处理各序列化方式的客户端请求调用的公共接口
     */
    interface RpcInvoker{
        /**
         * 服务端实现该方法，用来完成客户端请求的方法调用
         *
         * @param server RPC.Server对象，用来获取服务端的属性
         * @param protocol 客户端请求的接口（协议）
         * @param rpcRequest 客户端调用请求的封装类对象（反序列化后的）
         * @param receiveTime 开始本次RPC请求的时间
         * @return 方法调用的返回值
         * @throws Exception
         */
        Writable call(Server server, String protocol, Writable rpcRequest, long receiveTime) throws Exception;
    }

    /**
     * 根据RpcKind和Server.ProtoName对象获取对应接口实现的缓存对象
     *
     * @param rpcKind 序列化方式
     * @param server RPC.Server对象，
     *               含有{@link RPC.Server.ProtoName}作为key，{@link RPC.Server.ProtoClassProtoImpl}作为value的HashMap
     *               以及获取它们的方法
     * @param protocolName 协议名，是{@link RPC.Server.ProtoName}中的属性
     * @return RpcKind和Server.ProtoName对象映射出的接口实现的缓存对象
     * @throws RpcServerException
     */
    static Server.ProtoClassProtoImpl getProtocolImp(
            RPC.RpcKind rpcKind,
            RPC.Server server,
            String protocolName
    )throws RpcServerException{
        Server.ProtoName pn = new Server.ProtoName(protocolName);
        Server.ProtoClassProtoImpl impl = server.getProtocolImplMap(rpcKind).get(pn);
        if (impl == null){
            throw new RpcNoSuchMethodException("Unknown protocol: " + protocolName);
        }
        return impl;
    }

}
