import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BlockReceiver {

    private static final String DATA_NODE_DIR = "customDataNodeStorage";
    private int listenPort;

    public BlockReceiver(int listenPort) {
        this.listenPort = listenPort;
    }

    public void start() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            System.out.println("BlockReceiver listening on port " + listenPort);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                try {
                    receiveBlock(clientSocket);
                } catch (IOException e) {
                    System.out.println("Error receiving block: " + e.getMessage());
                }
            }
        }
    }

    private void receiveBlock(Socket clientSocket) throws IOException {
        try (DataInputStream clientInputStream = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream clientOutputStream = new DataOutputStream(clientSocket.getOutputStream())) {

            // Read the WRITE command and the blockId
            String command = clientInputStream.readUTF();
            String[] commandParts = command.split(" ");
            if (!"WRITE".equals(commandParts[0])) {
                throw new IOException("Invalid command: " + command);
            }
            String blockId = commandParts[1];

            // Read the block size and the block data
            int blockSize = clientInputStream.readInt();
            byte[] buffer = new byte[4096];
            Path blockPath = Paths.get(DATA_NODE_DIR, blockId);
            Files.createDirectories(blockPath.getParent());
            try (FileOutputStream blockOutputStream = new FileOutputStream(blockPath.toString())) {
                int bytesRead;
                int remainingBytes = blockSize;
                while (remainingBytes > 0 && (bytesRead = clientInputStream.read(buffer, 0, Math.min(buffer.length, remainingBytes))) != -1) {
                    blockOutputStream.write(buffer, 0, bytesRead);
                    remainingBytes -= bytesRead;
                }
            }

            System.out.println("Block " + blockId + " received and saved.");
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Usage: BlockReceiver <listenPort>");
            System.exit(1);
        }

        int listenPort = Integer.parseInt(args[0]);
        BlockReceiver blockReceiver = new BlockReceiver(listenPort);
        blockReceiver.start();
    }
}