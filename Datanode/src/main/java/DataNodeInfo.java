package BlockManagment;

public class DataNodeInfo {

    private final String address;
    private final int port;

    public DataNodeInfo(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }
}