package com.rakuten.rit.roma.romac4j.routing;

import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.Receiver;
import com.rakuten.rit.roma.romac4j.StringReceiver;
import com.rakuten.rit.roma.romac4j.ValueReceiver;
import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;

public final class Routing extends Thread {
    protected static Logger log = Logger.getLogger(Routing.class.getName());
    private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
    private Random rnd = new Random(System.currentTimeMillis());
    private String[] initialNodes = null;
    private HashMap<String, Integer> failCountMap = new HashMap<String, Integer>();
    volatile private boolean threadLoop = false;
    volatile private RoutingData routingData = null;

    private int failCount = 10;
    private int threadSleep = 5000;

    public Routing(String nodeId) {
        initialNodes = new String[] { nodeId };
    }

    public Routing(String[] nodes) {
        initialNodes = nodes;
    }

    public void run() {
        if (threadLoop) {
            log.warn("routing check thread : already running.");
            return;
        }
        log.info("routing check thread : started");
        threadLoop = true;
        while (threadLoop == true) {
            try {
                String myHash = null;
                String romaHash = getMklHash();
                if (romaHash != null) {
                    if (routingData != null) {
                        synchronized(this){
                            myHash = routingData.getMklHash("0");
                        }
                    }
                    if (!romaHash.equals(myHash)) {
                        RoutingData buf = getRoutingDump();
                        if (buf != null) {
                            synchronized(this){
                                routingData = buf;
                            }
                            synchronized (failCountMap) {
                                failCountMap.clear();
                            }
                            log.info("Routing has changed.");
                        }
                    }
                }
                Thread.sleep(threadSleep);
            } catch (Exception e) {
                log.debug("routing check thread : " + e.getMessage());
            }
        }
        log.info("routing check thread : stopped");
    }

    public void stopThread() {
        threadLoop = false;
    }

    public Connection getConnection() throws Exception {
        if (routingData != null) {
            return (sps.getConnection(routingData.getRandomNodeId()));
        } else {
            int n = rnd.nextInt(initialNodes.length);
            return (sps.getConnection(initialNodes[n]));
        }
    }

    public Connection getConnection(String key) throws Exception {
        String nid = null;
        try {
            synchronized(this){
                nid = routingData.getPrimaryNodeId(key);
            }
            log.debug("nid: " + nid);
            if (nid == null) {
                log.error("getConnection() : can't get a primary node. key = "
                        + key);
                return getConnection();
            }
            return (sps.getConnection(nid));
        } catch (NoSuchAlgorithmException ex) {
            log.error("getConnection() : " + ex.getMessage());
            // fatal error : stop an application
            throw new RuntimeException("fatal : " + ex.getMessage());
        } catch (Exception ex2){
            return new Connection(nid);
        }
    }

    public void returnConnection(Connection con) {
        synchronized (failCountMap) {
            failCountMap.remove(con.getNodeId());
        }
        sps.returnConnection(con);
    }

    public void failCount(Connection con) {
        failCount(con.getNodeId());
    }
    
    public void failCount(String nid) {
        log.debug("failCount: start");
        int n = 0;
        //String nid = con.getNodeId();
        log.debug("failCount: nid="+nid);
        synchronized (failCountMap) {
            log.debug("failCount: failCountMap nid="+nid);

            if (failCountMap.containsKey(nid)) {
                n = failCountMap.get(nid);
            }
            n++;
            log.debug("n: " + n);
            if (n >= failCount) {
                log.debug("failCount: failOver");
                failCountMap.clear();
                synchronized(this){
                    routingData = routingData.failOver(nid);
                }

                // TODO : will close connections for fail node in connection
                // pool.

            } else {
                failCountMap.put(nid, n);
            }
        }
        log.debug("failCount: end");
    }

    public void setFailCount(int n) {
        failCount = n;
    }

    public void setThreadSleep(int n) {
        threadSleep = n;
    }

    private String getMklHash() {
        Connection con = null;
        Receiver rcv = new StringReceiver();
        String nid = null;
        try {
            con = getConnection();
            nid = con.getNodeId();
            con.write("mklhash 0");
            rcv.receive(con);
            returnConnection(con);
        } catch (Exception e) {
            log.error("getMklHash() : " + e.getMessage());
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
        String nid = null;
        try {
            con = getConnection();
            nid = con.getNodeId();
            con.write("routingdump bin");
            rcv.receive(con);
            byte[] buff = ((ValueReceiver) rcv).getValue();
            routingData = new RoutingData(buff);
            returnConnection(con);
        } catch (ParseException e) {
            log.error("getRoutingDump() : " + e.getMessage());
            returnConnection(con);
            return null;
        } catch (Exception e) {
            log.warn("getRoutingDump() : " + e.getMessage());
            failCount(con);
            con.forceClose();
            return null;
        }
        return routingData;
    }
}
