package com.rakuten.rit.roma.romac4j;

import java.text.ParseException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.connection.Connection;
import com.rakuten.rit.roma.romac4j.connection.RomaSocketPool;
import com.rakuten.rit.roma.romac4j.routing.Routing;

public class RomaClient {
    protected static Logger log = Logger.getLogger(RomaClient.class.getName());
    private Routing routing = null;
    private int maxRetry = 5;
    
    public RomaClient(String nodeId) {
        BasicConfigurator.configure();

        try{
            RomaSocketPool.getInstance();
        }catch(RuntimeException e){
            RomaSocketPool.init();
            log.warn("RomaClient() : SocketPool initialized in RomaClient().");
        }
        
        routing = new Routing(nodeId);
        routing.start();
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error("RomaClient() : " + e.getMessage());
        }
    }

    public void destroy() {
        routing.stopThread();
    }

    public void setMaxRetry(int n) {
        maxRetry = n;
    }
    
    public void setFailCount(int n) {
        routing.setFailCount(n);
    }
    
    public void setThreadSleep(int n) {
        routing.setThreadSleep(n);
    }
    
    protected Receiver sendCmd(Receiver rcv, String cmd, String key,
            String opt, byte[] value) throws RetryOutException {
        return sendCmd(rcv, cmd, key, opt, value, -1);
    }

    protected Receiver sendCmd(Receiver rcv, String cmd, String key,
            String opt, byte[] value, int casid) throws RetryOutException {
        boolean retry;

        do {
            retry = false;
            Connection con = null;
            try {
                con = routing.getConnection(key);
                con.write(cmd, key, opt, value, casid);
                rcv.receive(con);
                routing.returnConnection(con);
            } catch (ParseException e) {
                routing.returnConnection(con);
                log.error("sendCmd(): " + e.getMessage());
                throw new RuntimeException(e);
            } catch (Exception e) {
                log.error("sendCmd(): " + e.getMessage());
                retry = true;
                log.debug("sendCmd(): retry=" + rcv.retry);
                routing.failCount(con);
                if (++rcv.retry >= maxRetry) {
                    log.error("sendCmd(): RetryOutException");
                    throw new RetryOutException();
                }
            }
        } while (retry);

        return rcv;
    }

    public byte[] get(String key) throws RetryOutException {
        Receiver rcv = sendCmd(new ValueReceiver(), "get", key, null, null);
        return ((ValueReceiver) rcv).getValue();
    }

    private boolean set(String cmd, String key, byte[] value, int expt)
            throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), cmd, key, "0 " + expt
                + " " + value.length, value);
        return rcv.toString().equals("STORED");
    }

    public boolean set(String key, byte[] value, int expt)
            throws RetryOutException {
        return set("set", key, value, expt);
    }

    public boolean add(String key, byte[] value, int expt)
            throws RetryOutException {
        return set("add", key, value, expt);
    }

    public boolean replace(String key, byte[] value, int expt)
            throws RetryOutException {
        return set("replace", key, value, expt);
    }

    public boolean append(String key, byte[] value, int expt)
            throws RetryOutException {
        return set("append", key, value, expt);
    }

    public boolean prepend(String key, byte[] value, int expt)
            throws RetryOutException {
        return set("prepend", key, value, expt);
    }

    public boolean incr(String key, int value) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "incr", key, "" + value,
                null);
        return rcv.toString().equals("STORED");
    }

    public boolean decr(String key, int value) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "decr", key, "" + value,
                null);
        return rcv.toString().equals("STORED");
    }

    public boolean delete(String key) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "delete", key, null, null);
        return rcv.toString().equals("DELETED");
    }

    public boolean setExpt(String key, int expt) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "set_expt", key,
                "" + expt, null);
        return rcv.toString().equals("STORED");
    }

    public boolean cas(String key, int expt, Cas callback)
            throws RetryOutException {
        Receiver rcv = sendCmd(new ValueReceiver(), "gets", key, null, null);
        int casid = 0;
        try {
            casid = ((ValueReceiver) rcv).getCasid();
        } catch (ParseException e) {
            log.error("cas() : " + e.getMessage());
            return false;
        }
        byte[] value = callback.cas((ValueReceiver) rcv);

        Receiver rcv2 = sendCmd(new StringReceiver(), "cas", key, "0 " + expt
                + " " + value.length, value, casid);
        return rcv2.toString().equals("STORED");
    }
}
