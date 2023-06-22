package org.DFSdemo.protocolTranslatorPB;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.DFSdemo.protocol.ClientProtocol;
import org.DFSdemo.protocol.proto.ClientNamenodeProtocolProtos;
import org.DFSdemo.protocolPB.ClientNamenodeProtocolPB;

import java.io.IOException;

/**
 * 该类实现了{@link ClientNamenodeProtocolPB}，是为了将ClientProtocol接口适配到RPC类型的PB接口，好为RPC接口创建实例化对象并保存
 *
 * 它将PB数据类型转到ClientProtocol接口中定义的数据类型
 */
public class ClientNamenodeProtocolServerSideTranslatorPB implements ClientNamenodeProtocolPB {

    final private ClientProtocol server;

    public ClientNamenodeProtocolServerSideTranslatorPB(ClientProtocol server){
        this.server = server;
    }

    @Override
    public ClientNamenodeProtocolProtos.rename2ResponseProto rename2(RpcController controller, ClientNamenodeProtocolProtos.rename2RequestProto request) throws ServiceException {
        String src = request.getSrc();
        String dst = request.getDst();

        try {
            boolean ret = server.rename2(src, dst);
            return ClientNamenodeProtocolProtos.rename2ResponseProto.newBuilder()
                    .setResult(ret)
                    .build();
        }catch (IOException e){
            throw new ServiceException(e);
        }
    }
}
