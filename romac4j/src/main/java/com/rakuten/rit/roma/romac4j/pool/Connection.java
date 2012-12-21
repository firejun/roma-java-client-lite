package com.rakuten.rit.roma.romac4j.pool;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.TimeoutException;

public class Connection extends Socket {
    private String nodeId;
    private String sendCmd;
    private InputStream is;

    public Connection() {
    }

    public void write(String cmd, String key, String opt, byte[] value,
            int casid) throws TimeoutException {
        // TODO

        // cmd + " " + key;
        // cmd + " " + key + " " + opt;
        // cmd + " " + key + " " + opt + " " + value.length;
        // cmd + " " + key + " " + opt + " " + value.length + " " + casid;

        if (cmd.equals("mklhash 0") || cmd.equals("routingdump bin")) {
            sendCmd = cmd;
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
        // BufferedInputStream is = null;

        byte[] b = new byte[1];
        // TODO: const...
        byte[] buff = new byte[1024];
        int i = 0;
        try {
            writer = new PrintWriter(getOutputStream(), true);

            writer.write(sendCmd + "\r\n");
            writer.flush();

            is = new BufferedInputStream(getInputStream());
            while (true) {
                // const...
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

    public byte[] readValue() {
        // BufferedInputStream is = null;
        int rtLen = 0;
        String str = readLine();
        if (sendCmd.equals("routingdump bin")) {
            rtLen = Integer.parseInt(str);
        } else {
            String[] header = str.split(" ");
            if (header.length == 4) {
                rtLen = Integer.valueOf(header[3]);
            }
            // throw Exception???
        }

        // Initialize buffer
        // TODO: const...
        byte[] b = new byte[1024];
        byte[] buff = new byte[rtLen + 7];

        int receiveCount = 0;
        int count = 0;
        try {
            while (receiveCount < rtLen + 7) {
                count = is.read(b, 0, 1024);
                System.arraycopy(b, 0, buff, receiveCount, count);
                receiveCount += count;
            }
        } catch (IOException e) {
            // e.printStackTrace();
        }
        return buff;
    }
}
