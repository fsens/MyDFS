package org.DFSdemo.ipc;

import org.DFSdemo.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public abstract class Server {

    public static final Log LOG = LogFactory.getLog(Server.class);
    private String bindAddress;
    private int port;
    private int handlerCount;
    private int readThread;

    volatile private boolean running = true;
    private Configuration conf;

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

    }

    /**
     * 启动服务
     */
    public void start(){

    }

    public synchronized void join() throws InterruptedException{
        while (running){
            wait();
        }
    }

}
