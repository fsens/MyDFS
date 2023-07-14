import com.google.protobuf.Message;
import org.DFSdemo.protocol.proto.ClientNamenodeProtocolProtos;
import org.DFSdemo.protocol.proto.ProtobufRpcEngineProtos;
import org.DFSdemo.util.ProtoUtil;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class testSerDeser {

    @Parameterized.Parameters
    public static Collection<Object[]> testData(){
        Object[][] data = readExcel("TestCases.xlsx", 0);
        return Arrays.asList(data);
    }

    /** 未序列化的请求 */
    private String methodName;
    private String declaringClassProtocolName;
    private String src;
    private String dst;
    /** 反序列化后的请求，即预期结果 */
    private String expected_methodName;
    private String expected_declaringClassProtocolName;
    private String expected_src;
    private String expected_dst;
    public testSerDeser(String methodName, String declaringClassProtocolName, String src, String dst,
                        String expected_methodName, String expected_declaringClassProtocolName, String expected_src, String expected_dst){
        this.methodName = methodName;
        this.declaringClassProtocolName = declaringClassProtocolName;
        this.src = src;
        this.dst = dst;
        this.expected_methodName = expected_methodName;
        this.expected_declaringClassProtocolName = expected_declaringClassProtocolName;
        this.expected_src = expected_src;
        this.expected_dst = expected_dst;
    }


    @Test
    public void testRpcRequestWrapper() throws Exception{
        /**
         * 创建并设置header和req的值
         */
        ProtobufRpcEngineProtos.RequestHeaderProto header = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder()
                .setMethodName(methodName)
                .setDeclaringClassProtocolName(declaringClassProtocolName)
                .build();
        ClientNamenodeProtocolProtos.rename2RequestProto req = ClientNamenodeProtocolProtos.rename2RequestProto.newBuilder()
                .setSrc(src)
                .setDst(dst)
                .build();

        /**
         * 序列化header和req
         */
        Class clazz = Class.forName("org.DFSdemo.ipc.ProtobufRpcEngine$RpcRequestWrapper");
        Constructor constructor = clazz.getConstructor(ProtobufRpcEngineProtos.RequestHeaderProto.class, Message.class);
        constructor.setAccessible(true);
        Object rpcReqWrapper = constructor.newInstance(header, req);
        System.out.println("被序列化的协议和方法 :" + rpcReqWrapper);

        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);//因为write方法参数是DataOutput类型的，所有要将输出流转为DataOutput或其子类类型
        Method writeMethod = clazz.getMethod("write",DataOutput.class);
        writeMethod.setAccessible(true);
        writeMethod.invoke(rpcReqWrapper, out);//序列化
        System.out.println("序列化后的长度: " + bo.toByteArray().length);
        System.out.print("\n");

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
        Assert.assertEquals(expected_methodName, header.getMethodName());
        Assert.assertEquals(expected_declaringClassProtocolName, header.getDeclaringClassProtocolName());

        /**
         * 反序列化req，进行单元测试
         */
        byte[] theRequestRead = (byte[]) theRequestReadField.get(newRpcReqWrapper);
        ClientNamenodeProtocolProtos.rename2RequestProto deSerReq = ClientNamenodeProtocolProtos.rename2RequestProto.parseFrom(theRequestRead);
        Assert.assertEquals(expected_src, deSerReq.getSrc());
        Assert.assertEquals(expected_dst, deSerReq.getDst());
    }

    //获取excel表的数据
    public static Object[][] readExcel(String file,int sheet_index){
        Workbook workbook = null;
        Object[][] data = null;
        Row row=null;
        Cell cell=null;
        try {
            //得到workbook实例
            workbook = new XSSFWorkbook(new FileInputStream(file));
            //得到sheet
            Sheet sheet = workbook.getSheetAt(sheet_index);
            //得到该sheet的行数和列数
            int row_num = sheet.getPhysicalNumberOfRows();
            int cell_num = sheet.getRow(0).getPhysicalNumberOfCells();
            data = new Object[row_num-1][cell_num];

            //读取excel表内容
            for (int i = 1;i<row_num;i++){
                row = sheet.getRow(i);
                for (int j = 0; j < cell_num; j++) {
                    data[i-1][j]=row.getCell(j).getStringCellValue();
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }
}
