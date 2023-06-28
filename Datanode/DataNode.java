import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class DataNode {

    private static final String DATA_NODE_DIR = "customDataNodeStorage";
    private static final int DATA_NODE_PORT = 9000;
    private final BlockPoolManager blockPoolManager;

    public DataNode() {
        blockPoolManager = new BlockPoolManager();
    }

    public static void main(String[] args) throws IOException {
        DataNode dataNode = new DataNode();
        createStorageDirectory();
        startDataNodeServer(dataNode);
    }

    private static void createStorageDirectory() {
        File dataNodeDir = new File(DATA_NODE_DIR);
        if (!dataNodeDir.exists()) {
            dataNodeDir.mkdirs();
        }
    }

    private static void startDataNodeServer(DataNode dataNode) throws IOException {
        ServerSocket serverSocket = new ServerSocket(DATA_NODE_PORT);
        System.out.println("DataNode started on port " + DATA_NODE_PORT);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Received connection from " + clientSocket.getInetAddress());
            new Thread(new DataNodeClientHandler(clientSocket, dataNode)).start();
        }
    }

    public Path getBlockPath(String blockId) {
        return blockPoolManager.getBlockPath(blockId);
    }

    public void addBlock(Block block) {
        blockPoolManager.addBlock(block);
    }

    public void removeBlock(String blockId) {
        blockPoolManager.removeBlock(blockId);
    }

    static class BlockPoolManager {

        private static final String BLOCK_POOL_DIR = "blockPoolStorage";
        private final Map<String, BlockInfo> blockMap;
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
            BlockInfo blockInfo = blockMap.get(blockId);
            if (blockInfo != null) {
                return Paths.get(BLOCK_POOL_DIR, blockInfo.getBlock().getBlockId());
            }
            return null;
        }

        public void addBlock(Block block) {
            BlockInfo blockInfo = new BlockInfo(block, 1);
            blockMap.put(block.getBlockId(), blockInfo);
        }

        public void removeBlock(String blockId) {
            blockMap.remove(blockId);
        }
    }

    static class DataNodeClientHandler implements Runnable {

        private final Socket clientSocket;
        private final DataNode dataNode;

        DataNodeClientHandler(Socket clientSocket, DataNode dataNode) {
            this.clientSocket = clientSocket;
            this.dataNode = dataNode;
        }

        @Override
        public void run() {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                String command = reader.readLine();
                processCommand(command);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void processCommand(String command) throws IOException {
            String[] commandParts = command.split(" ");
            String operation = commandParts[0];

            switch (operation) {
                case "COPY":
                    copyBlock(commandParts[1], commandParts[2]);
                    break;
                case "DELETE":
                    deleteBlock(commandParts[1]);
                    break;
                default:
                    System.out.println("Unknown command: " + command);
            }
        }

        private void copyBlock(String src, String dest) throws IOException {
            Path srcPath = dataNode.getBlockPath(src);
            Path destPath = Paths.get(DATA_NODE_DIR, dest);
            System.out.println("Copying block from " + srcPath + " to " + destPath);

            // Assuming the size of the block is the size of the file
            long size = Files.size(srcPath);
            long timestamp = System.currentTimeMillis();
            Block newBlock = new Block(dest, size, timestamp);
            Files.copy(srcPath, destPath);
            dataNode.addBlock(newBlock);
        }

        private void deleteBlock(String block) throws IOException {
            Path blockPath = dataNode.getBlockPath(block);
            System.out.println("Deleting block " + blockPath);
            Files.delete(blockPath);
            dataNode.removeBlock(block);
        }
    }
}
