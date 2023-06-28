import java.util.HashMap;
import java.util.Map;

public class HeartbeatManager {
    private final Map<String, Long> heartbeatMap; // 存储节点ID和最近心跳时间的映射

    public HeartbeatManager() {
        this.heartbeatMap = new HashMap<>();
    }

    /**
     * 处理心跳消息
     *
     * @param nodeId 存储节点ID
     */
    public void processHeartbeat(String nodeId) {
        heartbeatMap.put(nodeId, System.currentTimeMillis());
    }

    /**
     * 检查存储节点是否活跃（即最近是否有心跳）
     *
     * @param nodeId 存储节点ID
     * @return true表示存储节点活跃，false表示存储节点不活跃
     */
    public boolean isNodeActive(String nodeId) {
        return heartbeatMap.containsKey(nodeId);
    }

    /**
     * 获取存储节点的最近心跳时间
     *
     * @param nodeId 存储节点ID
     * @return 最近心跳时间（毫秒）
     */
    public long getHeartbeatTime(String nodeId) {
        return heartbeatMap.getOrDefault(nodeId, 0L);
    }

    /**
     * 移除存储节点的心跳信息
     *
     * @param nodeId 存储节点ID
     */
    public void removeHeartbeat(String nodeId) {
        heartbeatMap.remove(nodeId);
    }
}