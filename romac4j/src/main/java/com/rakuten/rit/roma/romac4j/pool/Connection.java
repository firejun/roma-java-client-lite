package com.rakuten.rit.roma.romac4j.pool;

import java.net.Socket;
import java.util.concurrent.TimeoutException;

public class Connection extends Socket {
    private String nodeId;

    public Connection() {
    }

    public void write(String cmd, String key, String opt, byte[] value, int casid) throws TimeoutException {
        // TODO

        //cmd + " " + key;
        //cmd + " " + key + " " + opt;
        //cmd + " " + key + " " + opt + " " + value.length;
        //cmd + " " + key + " " + opt + " " + value.length + " " + casid;
        
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String readLine() {
        
        return null;
    }
}
