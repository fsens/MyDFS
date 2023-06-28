package BlockManagment;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BlockSender {

    private static final String DATA_NODE_DIR = "customDataNodeStorage";

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: CustomBlockSender <blockId> <targetHost> <targetPort>");
            System.exit(1);
        }

        String blockId = args[0];
        String targetHost = args[1];
        int targetPort = Integer.parseInt(args[2]);

        sendBlock(blockId, targetHost, targetPort);
    }

    private static void sendBlock(String blockId, String targetHost, int targetPort) throws IOException {
        Path blockPath = Paths.get(DATA_NODE_DIR, blockId);
        if (!Files.exists(blockPath)) {
            System.out.println("Block not found: " + blockId);
            return;
        }

        try (Socket targetSocket = new Socket(targetHost, targetPort);
             DataInputStream blockInputStream = new DataInputStream(new FileInputStream(blockPath.toString()));
             DataOutputStream targetOutputStream = new DataOutputStream(targetSocket.getOutputStream())) {

            // Send the WRITE command and the blockId to the target DataXceiver
            targetOutputStream.writeUTF("WRITE " + blockId);

            // Send the block size and the block data
            int blockSize = (int) Files.size(blockPath);
            targetOutputStream.writeInt(blockSize);
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = blockInputStream.read(buffer)) != -1) {
                targetOutputStream.write(buffer, 0, bytesRead);
            }

            System.out.println("Block " + blockId + " sent to " + targetHost + ":" + targetPort);
        }
    }
}