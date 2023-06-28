// HeartbeatMonitor.java

public class HeartbeatMonitor {
    private final HeartbeatManager heartbeatManager;

    public HeartbeatMonitor(HeartbeatManager heartbeatManager) {
        this.heartbeatManager = heartbeatManager;
    }

    /**
     * 监视存储节点的心跳状态
     *
     * @param nodeId    存储节点ID
     * @param threshold 心跳超时阈值（毫秒）
     * @return true如果心跳正常，否则false
     */
    public boolean monitorHeartbeat(String nodeId, long threshold) {
        long lastHeartbeatTime = heartbeatManager.getHeartbeatTime(nodeId);
        long currentTime = System.currentTimeMillis();

        if (currentTime - lastHeartbeatTime > threshold) {
            // 超过心跳超时阈值，心跳异常
            return false;
        }

        // 心跳正常
        return true;
    }
}