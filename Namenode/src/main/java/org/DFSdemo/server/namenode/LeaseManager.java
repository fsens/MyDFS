import java.util.HashMap;
import java.util.Map;

public class LeaseManager {
    private final Map<String, Lease> leaseMap; // 存储节点ID和租约的映射
    private final long leaseDuration; // 租约持续时间（毫秒）

    public LeaseManager(long leaseDuration) {
        this.leaseMap = new HashMap<>();
        this.leaseDuration = leaseDuration;
    }

    /**
     * 获取租约
     *
     * @param nodeId 存储节点ID
     * @return 存储节点对应的租约
     */
    public Lease getLease(String nodeId) {
        if (leaseMap.containsKey(nodeId)) {
            return leaseMap.get(nodeId);
        } else {
            Lease lease = new Lease(nodeId, null, 0L);
            leaseMap.put(nodeId, lease);
            return lease;
        }
    }

    /**
     * 刷新租约的心跳时间
     *
     * @param lease 租约对象
     */
    public void renewLease(Lease lease) {
        lease.setExpirationTime(System.currentTimeMillis() + leaseDuration);
    }

    /**
     * 检查租约是否过期
     *
     * @param lease 租约对象
     * @return 如果租约过期，则返回true，否则返回false
     */
    public boolean isLeaseExpired(Lease lease) {
        return lease.getExpirationTime() < System.currentTimeMillis();
    }

    /**
     * 移除租约
     *
     * @param lease 租约对象
     */
    public void removeLease(Lease lease) {
        leaseMap.remove(lease.getPath());
    }
}
