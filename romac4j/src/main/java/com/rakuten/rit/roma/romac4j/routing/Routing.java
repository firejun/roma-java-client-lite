package com.rakuten.rit.roma.romac4j.routing;

import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.Receiver;
import com.rakuten.rit.roma.romac4j.StringReceiver;
import com.rakuten.rit.roma.romac4j.ValueReceiver;
import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.utils.StringUtils;

public final class Routing extends Thread {
    protected static Logger log = Logger.getLogger(Routing.class.getName());
    private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
    private Properties props;
    private String mklHash;
    private RoutingData routingData;
    private Random rnd = new Random(System.currentTimeMillis());
    // private int rndVal;
    private boolean status = false;

    /**
     * 
     * @param props
     */
    public Routing(Properties props) {
        this.props = props;
    }

    /**
	 * 
	 */
    public void run() {
        while (status == false) {
            try {
                String mklHash = getMklHash();
                if (mklHash != null && !mklHash.equals(this.mklHash)) {
                    this.mklHash = mklHash;
                    RoutingData tempBuff = getRoutingDump();
                    synchronized (routingData) {
                        routingData = tempBuff;
                    }
                    log.debug("Routing change!");
                } else {
                    log.debug("Routing no change!");
                }
            } catch (Exception e) {
                log.debug("run() Error.");
                e.printStackTrace();
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * 
     * @param key
     * @return long vn
     * @throws NoSuchAlgorithmException
     */
    public long getVn(String key) throws NoSuchAlgorithmException {
        int divBits = 0;
        int dgstBits = 0;
        synchronized (routingData) {
            divBits = routingData.getDivBits();
            dgstBits = routingData.getDgstBits();
        }
        long mask = ((1L << divBits) - 1) << (dgstBits - divBits);
        MessageDigest md = MessageDigest.getInstance("SHA1");
        md.update(key.getBytes());
        byte[] b = md.digest();
        long h = ((long) b[b.length - 7] << 48) & 0xff000000000000L
                | ((long) b[b.length - 6] << 40) & 0xff0000000000L
                | ((long) b[b.length - 5] << 32) & 0xff00000000L
                | ((long) b[b.length - 4] << 24) & 0xff000000L
                | ((long) b[b.length - 3] << 16) & 0xff0000L
                | ((long) b[b.length - 2] << 8) & 0xff00L
                | (long) b[b.length - 1] & 0xffL;
        return h & mask;
    }

    // /**
    // *
    // * @return routingData
    // */
    // public RoutingData getRoutingData() {
    // synchronized (routingData) {
    // return routingData;
    // }
    // }

    /**
     * 
     * @param status
     */
    public void setStatus(boolean status) {
        this.status = status;
    }

    public Connection getConnection() {
        Connection con = null;
        String[] nodeId = null;
        int rndVal = rnd.nextInt(routingData.getNumOfNodes());
        try {
            synchronized (routingData) {
                nodeId = routingData.getNodeId();
            }
            con = sps.getConnection(nodeId[rndVal]);
        } catch (Exception ex) {
            // TODO: Exception throw??
        }
        return con;
    }

    public Connection getConnection(String key) {
        // TODO
        Connection con = null;
        String[] nodeId = null;
        int[] vNode = null;
        try {
            long vn = getVn(key);
            synchronized (routingData) {
                nodeId = routingData.getNodeId();
                vNode = routingData.getVNode().get(vn);
            }
            con = sps.getConnection(nodeId[0]);
            con.setNodeId(nodeId[vNode[0]]);
        } catch (NoSuchAlgorithmException ex) {
            // TODO: Exception throw??
        }

        return con;
    }

    public void returnConnection(Connection con) {
        // TODO
        sps.returnConnection(con);
    }

    public void failCount(Connection con) {
        // TODO

    }

    private String getMklHash() {
        Connection con = null;
        Receiver rcv = new StringReceiver();
        try {
            con = getConnection();
            con.write("mklhash 0", null, null, null, -1);
            rcv.receive(con);
            returnConnection(con);
        } catch (TimeoutException e) {
        }
        return rcv.toString();
    }

    private RoutingData getRoutingDump() {
        RoutingData routingData = null;
        Receiver rcv = new ValueReceiver();
        byte[] buff = null;
        try {
            Connection con = getConnection();
            con.write("routingdump bin", null, null, null, -1);
            rcv.receive(con);
            buff = ((ValueReceiver)rcv).getValue();
            
            routingData = new RoutingData(buff);
            
            returnConnection(con);
        } catch (Exception e) {
            e.printStackTrace();
            // TODO:
            //throw new Exception("RoutingDump Exception.");
        }
        return routingData;
    }
}
