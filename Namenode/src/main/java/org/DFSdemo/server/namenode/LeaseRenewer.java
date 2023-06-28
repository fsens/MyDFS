public class LeaseRenewer {
    private final LeaseManager leaseManager;

    public LeaseRenewer(LeaseManager leaseManager) {
        this.leaseManager = leaseManager;
    }

    /**
     * 开启租约续约线程
     *
     * @param lease 租约对象
     */
    public void startLeaseRenewalThread(Lease lease) {
        Thread renewalThread = new Thread(() -> {
            while (true) {
                // 检查租约是否过期
                if (leaseManager.isLeaseExpired(lease)) {
                    // 租约已过期，执行相应操作
                    System.out.println("Lease expired: " + lease.getPath());
                    break;
                }

                // 刷新租约的心跳时间
                leaseManager.renewLease(lease);

                // 休眠一段时间后再次续约
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        renewalThread.start();
    }
}
