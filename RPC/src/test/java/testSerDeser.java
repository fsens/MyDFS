import com.google.protobuf.Message;
import org.DFSdemo.protocol.proto.ClientNamenodeProtocolProtos;
import org.DFSdemo.protocol.proto.ProtobufRpcEngineProtos;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class testSerDeser {

    @Test
    public void testRpcRequestWrapper() throws Exception{
        /**
         * 创建并设置header和req的值
         */
        ProtobufRpcEngineProtos.RequestHeaderProto header = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder()
                .setMethodName("testRpcRequestWrapper")
                .setDeclaringClassProtocolName("testProtocolName")
                .build();
        ClientNamenodeProtocolProtos.rename2RequestProto req = ClientNamenodeProtocolProtos.rename2RequestProto.newBuilder()
                .setSrc("testSrc")
                .setDst("testDst")
                .build();

        /**
         * 序列化header和req
         */
        Class clazz = Class.forName("org.DFSdemo.ipc.ProtobufRpcEngine$RpcRequestWrapper");
        Constructor constructor = clazz.getConstructor(ProtobufRpcEngineProtos.RequestHeaderProto.class, Message.class);
        constructor.setAccessible(true);
        Object rpcReqWrapper = constructor.newInstance(header, req);
        System.out.println(rpcReqWrapper);

        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);//因为write方法参数是DataOutput类型的，所有要将输出流转为DataOutput或其子类类型
        Method writeMethod = clazz.getMethod("write",DataOutput.class);
        writeMethod.setAccessible(true);
        writeMethod.invoke(rpcReqWrapper, out);//序列化
        System.out.println("write size" + bo.toByteArray().length);

        /**
         * 反序列化header
         */
        Constructor defaultConstructor = clazz.getConstructor();
        defaultConstructor.setAccessible(true);//获取无参构造器
        Object newRpcReqWrapper = defaultConstructor.newInstance();

        Field theRequestReadField = clazz.getSuperclass().getDeclaredField("theRequestRead");//获取父类的theRequestRead字段
        theRequestReadField.setAccessible(true);

        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);
        Method readFieldsMethod = clazz.getMethod("readFields", DataInput.class);
        readFieldsMethod.setAccessible(true);
        //反序列化。
        //由于RpcRequestWrapper类中对于theRequest的反序列化未完成，所以需要proto类来反序列化
        readFieldsMethod.invoke(newRpcReqWrapper, in);

        /**
         * header反序列化已经完成，现在进行单元测试
         */
        Assert.assertEquals("testRpcRequestWrapper", header.getMethodName());
        Assert.assertEquals("testProtocolName", header.getDeclaringClassProtocolName());

        /**
         * 反序列化req，进行单元测试
         */
        byte[] theRequestRead = (byte[]) theRequestReadField.get(newRpcReqWrapper);
        ClientNamenodeProtocolProtos.rename2RequestProto deSerReq = ClientNamenodeProtocolProtos.rename2RequestProto.parseFrom(theRequestRead);
        Assert.assertEquals("testSrc", deSerReq.getSrc());
        Assert.assertEquals("testDst", deSerReq.getDst());
    }
}
