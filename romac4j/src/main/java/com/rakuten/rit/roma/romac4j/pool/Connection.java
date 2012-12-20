package com.rakuten.rit.roma.romac4j.pool;

import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

public class Connection extends Socket {
    private String nodeId;
    private String sendStr;

    public Connection() {
    }

    public void write(String cmd, String key, String opt, byte[] value,
            int casid) throws TimeoutException {
        // TODO

        // cmd + " " + key;
        // cmd + " " + key + " " + opt;
        // cmd + " " + key + " " + opt + " " + value.length;
        // cmd + " " + key + " " + opt + " " + value.length + " " + casid;

        if (cmd.equals("mklhash 0")) {
            sendStr = "mklhash 0";
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String readLine() {
        PrintWriter writer = null;
        BufferedInputStream is = null;

        byte[] b = new byte[1];
        byte[] buff = new byte[1024];
        int i = 0;
        try {
            writer = new PrintWriter(getOutputStream(), true);

            writer.write(sendStr + new String(new byte[] { 0x0a, 0x0d }));
            writer.flush();

            is = new BufferedInputStream(getInputStream());
            while (true) {
                if (i > 1024) {
                    throw new ArrayIndexOutOfBoundsException("Too much size.");
                }
                is.read(b, 0, 1);
                if (b[0] == 0x0d) {
                    is.read(b, 0, 1);
                    if (b[0] == 0x0a)
                        break;
                }
                buff[i] = b[0];
                i++;
            }
        } catch (Exception e) {
            // throw new Exception("Can't convert header.");
            e.printStackTrace();
        }
        return new String(buff, 0, i);
    }
}
