package com.rakuten.rit.roma.romac4j.pool;

import java.net.Socket;
import java.util.concurrent.TimeoutException;

public class Connection extends Socket {
    String nodeId;
    
    public void write(String cmd, String key, String opt, byte[] value) throws TimeoutException{
        // TODO
    }
}
