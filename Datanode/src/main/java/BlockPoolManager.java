package BlockManagment;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BlockPoolManager {

    private static final String BLOCK_POOL_DIR = "blockPoolStorage";
    private final Map<String, Block> blockMap;
    private final AtomicLong nextBlockId;

    public BlockPoolManager() {
        blockMap = new HashMap<>();
        nextBlockId = new AtomicLong(1);
        initBlockPool();
    }

    private void initBlockPool() {
        // TODO: Load existing block mappings and set nextBlockId accordingly.
    }

    public String allocateNewBlockId() {
        return "block-" + nextBlockId.getAndIncrement();
    }

    public Path getBlockPath(String blockId) {
        Block block = blockMap.get(blockId);
        if (block != null) {
            return Paths.get(BLOCK_POOL_DIR, block.getBlockId());
        }
        return null;
    }

    public void addBlock(Block block) {
        blockMap.put(block.getBlockId(), block);
    }

    public void removeBlock(String blockId) {
        blockMap.remove(blockId);
    }

    public static void main(String[] args) {
        BlockPoolManager blockPoolManager = new BlockPoolManager();
        String newBlockId = blockPoolManager.allocateNewBlockId();
        Block newBlock = new Block(newBlockId, 0, System.currentTimeMillis());
        blockPoolManager.addBlock(newBlock);
        System.out.println("BlockPoolManager initialized with blocks: " + blockPoolManager.blockMap.keySet());
    }
}