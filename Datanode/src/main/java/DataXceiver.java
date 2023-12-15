package BlockManagment;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DataXceiver {

    private static final int DATA_XCEIVER_PORT = 9000;
    private static final String DATA_NODE_DIR = "customDataNodeStorage";

    public static void main(String[] args) throws IOException {
        startDataXceiverServer();
    }

    private static void startDataXceiverServer() throws IOException {
        ServerSocket serverSocket = new ServerSocket(DATA_XCEIVER_PORT);
        System.out.println("DataXceiver started on port " + DATA_XCEIVER_PORT);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            System.out.println("Received connection from " + clientSocket.getInetAddress());
            new Thread(new DataXceiverClientHandler(clientSocket)).start();
        }
    }

    static class DataXceiverClientHandler implements Runnable {

        private final Socket clientSocket;

        DataXceiverClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
                DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());

                String command = dis.readUTF();
                String[] commandParts = command.split(" ");
                String operation = commandParts[0];

                switch (operation) {
                    case "READ":
                        readFileBlock(commandParts[1], dos);
                        break;
                    case "WRITE":
                        writeFileBlock(commandParts[1], dis);
                        break;
                    default:
                        System.out.println("Unknown command: " + command);
                }
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

        private void readFileBlock(String blockId, DataOutputStream dos) throws IOException {
            Path blockPath = Paths.get(DATA_NODE_DIR, blockId);
            byte[] blockData = Files.readAllBytes(blockPath);
            dos.writeInt(blockData.length);
            dos.write(blockData);
            dos.flush();
        }

        private void writeFileBlock(String blockId, DataInputStream dis) throws IOException {
            Path blockPath = Paths.get(DATA_NODE_DIR, blockId);
            int blockSize = dis.readInt();
            byte[] blockData = new byte[blockSize];
            dis.readFully(blockData);
            Files.write(blockPath, blockData);

            System.out.println("Block " + blockId + " received from " + clientSocket.getInetAddress());
        }
    }
}
