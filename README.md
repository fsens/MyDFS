# RPC模块
## 1.整体架构
RPC框架分为了三层：![image](https://github.com/fsens/MyDFS/assets/95872817/cb2fa3d3-6414-410a-bab6-3d3091125c7d)

一次RPC调用流程：![image](https://github.com/fsens/MyDFS/assets/95872817/976cc54a-1c99-4942-8e1d-4bb6737f58b1)

服务端架构：![image](https://github.com/fsens/MyDFS/assets/95872817/791e96ed-58bc-4c61-a6b6-5569bede7aeb)

## 2.应用层协议
### 建立连接发送的信息
```
------------------Header----------------------
myrpc(5字节)：应用层协议类型，和HTTP等进行区分
Service Class(1字节)：服务类型，指Client和NameNode，或者DataNode与NameNode等服务
AuthProtocol(1字节)：客户端身份认证协议


-------------------Content--------------------
---content header---
RpcKindPtoto：序列化类型
OperationProto：指示服务端连接的操作
callId：唯一标识一个client发来的call
clientId：唯一标识客户端
retryCount：一个call的重试次数

---content content---
protocol：协议接口
```

### 一次rpc发送的信息
```
------------------Header--------------------
---header header---
RpcKindPtoto：序列化类型
OperationProto：指示服务端连接的操作
callId：唯一标识一个client发来的call
clientId：唯一标识客户端
retryCount：一个call的重试次数

---header content---
protocol：协议接口


-----------------Content---------------------
---content header---
methodName：方法名
declaringClassProtocolName：协议接口名

---content content---
请求参数
```

### rpc结果信息
```

```
