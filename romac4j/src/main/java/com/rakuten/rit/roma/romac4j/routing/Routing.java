package com.rakuten.rit.roma.romac4j.routing;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.Receiver;
import com.rakuten.rit.roma.romac4j.StringReceiver;
import com.rakuten.rit.roma.romac4j.ValueReceiver;
import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;

public final class Routing extends Thread {
    protected static Logger log = Logger.getLogger(Routing.class.getName());
    private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
    private String mklHash;
    private RoutingData routingData = null;
    private Random rnd = new Random(System.currentTimeMillis());
    private boolean status = false;
    private String[] initialNodes = null;

    /**
     * 
     * @param props
     */
    public Routing(String nodeId) {
        initialNodes = new String[] { nodeId };
    }

    public Routing(String[] nodes) {
        initialNodes = nodes;
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
                // TODO: const...
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
    }


    /**
     * 
     * @param status
     */
    public void setStatus(boolean status) {
        this.status = status;
    }

    public Connection getConnection() {
        if(routingData != null){
            int n = rnd.nextInt(routingData.getNumOfNodes());
            return(sps.getConnection(routingData.getNodeId()[n]));
        }else{
            int n = rnd.nextInt(initialNodes.length);
            return(sps.getConnection(initialNodes[n]));
        }
    }

    public Connection getConnection(String key) {
        try {
            String nid = routingData.getPrimaryNodeId(key);
            return(sps.getConnection(nid));
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("fatal:" + ex.getMessage());
        }
    }

    public void returnConnection(Connection con) {
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
            con.write("mklhash 0");
            rcv.receive(con);
            returnConnection(con);
        } catch (Exception e) {
            failCount(con);
            con.forceClose();
            return null;
        }
        return rcv.toString();
    }

    private RoutingData getRoutingDump() {
        Connection con = null;
        RoutingData routingData = null;
        Receiver rcv = new ValueReceiver();
        try {
            con = getConnection();
            con.write("routingdump bin");
            rcv.receive(con);
            byte[] buff = ((ValueReceiver)rcv).getValue();
            routingData = new RoutingData(buff);
            returnConnection(con);
        } catch (ParseException e) {
            log.error(e.getMessage());
            returnConnection(con);
            return null;
        } catch (Exception e) {
            log.warn(e.getMessage());
            failCount(con);
            con.forceClose();
            return null;
        }
        return routingData;
    }
}
