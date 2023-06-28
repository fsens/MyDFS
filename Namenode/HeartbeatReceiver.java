
public class HeartbeatReceiver {
    private final HeartbeatManager heartbeatManager;

    public HeartbeatReceiver(HeartbeatManager heartbeatManager) {
        this.heartbeatManager = heartbeatManager;
    }

    /**
     * 接收并处理存储节点发送的心跳消息
     *
     * @param nodeId 存储节点ID
     */
    public void receiveHeartbeat(String nodeId) {
        heartbeatManager.processHeartbeat(nodeId);
    }
}