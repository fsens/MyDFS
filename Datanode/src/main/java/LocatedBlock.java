package BlockManagment;

public class LocatedBlock {

    private final Block block;
    private final DataNodeInfo[] dataNodeLocations;

    public LocatedBlock(Block block, DataNodeInfo[] dataNodeLocations) {
        this.block = block;
        this.dataNodeLocations = dataNodeLocations;
    }

    public Block getBlock() {
        return block;
    }

    public DataNodeInfo[] getDataNodeLocations() {
        return dataNodeLocations;
    }

}
