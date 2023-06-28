public class Block {
    private final String blockId;
    private final long size;
    private final long timestamp;

    public Block(String blockId, long size, long timestamp) {
        this.blockId = blockId;
        this.size = size;
        this.timestamp = timestamp;
    }

    public String getBlockId() {
        return blockId;
    }

    public long getSize() {
        return size;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Block{" +
                "blockId='" + blockId + '\'' +
                ", size=" + size +
                ", timestamp=" + timestamp +
                '}';
    }
}
