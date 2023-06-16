package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
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
            Class<?> clazz = conf.getClass(RPC_ENGINE+"."+protocol.getName(),ProtobufRpcEngine.class);

        try {
            //通过反射实例化RpcEngine的实现类
            Constructor constructor = clazz.getDeclaredConstructor();
            engine = (RpcEngine) constructor.newInstance();
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

}
