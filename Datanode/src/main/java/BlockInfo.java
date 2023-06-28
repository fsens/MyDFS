package BlockManagment;

public class BlockInfo {

    private final Block block;
    private int replicationFactor;

    public BlockInfo(Block block, int replicationFactor) {
        this.block = block;
        this.replicationFactor = replicationFactor;
    }

    public Block getBlock() {
        return block;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }
}