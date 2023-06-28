import java.util.HashMap;
import java.util.Map;

public class BlockManager {

    private final Map<String, LocatedBlock> locatedBlocks;

    public BlockManager() {
        locatedBlocks = new HashMap<>();
    }

    public void addLocatedBlock(Block block, DataNodeInfo dataNodeInfo) {
        LocatedBlock locatedBlock = new LocatedBlock(block, new  DataNodeInfo[]{dataNodeInfo});
        locatedBlocks.put(block.getBlockId(), locatedBlock);
    }

    public LocatedBlock getLocatedBlock(String blockId) {
        return locatedBlocks.get(blockId);
    }

    public void removeLocatedBlock(String blockId) {
        locatedBlocks.remove(blockId);
    }
}