package com.rakuten.rit.roma.romac4j;

import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;

public class RomaClient {
    protected static Logger log = Logger.getLogger(RomaClient.class.getName());
    private Routing routing = null;
    private int maxRetry = 5;

    public RomaClient(Properties props) {
        BasicConfigurator.configure();

        try{
            SocketPoolSingleton.getInstance();
        }catch(RuntimeException e){
            SocketPoolSingleton.init(
                    Integer.parseInt(props.getProperty("maxActive")),
                    Integer.parseInt(props.getProperty("maxIdle")),
                    Integer.parseInt(props.getProperty("timeout")),
                    Integer.parseInt(props.getProperty("bufferSize")));
            log.warn("RomaClient() : SocketPool initialized in RomaClient().");
        }
        
        maxRetry = Integer.parseInt(props.getProperty("maxRetry"));
        
        routing = new Routing(props.getProperty("address_port"));
        routing.setFailCount(Integer.parseInt(props.getProperty("failCount")));
        routing.setThreadSleep(Integer.parseInt(props.getProperty("threadSleep")));
        routing.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            log.error("RomaClient() : " + e.getMessage());
        }
   }
    
    public RomaClient(String nodeId) {
        BasicConfigurator.configure();

        try{
            SocketPoolSingleton.getInstance();
        }catch(RuntimeException e){
            SocketPoolSingleton.init();
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
            String opt, byte[] value) throws IOException {
        return sendCmd(rcv, cmd, key, opt, value, -1);
    }

    protected Receiver sendCmd(Receiver rcv, String cmd, String key,
            String opt, byte[] value, int casid) throws IOException {
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
                    log.error("sendCmd(): Retry out");
                    throw new IOException("Retry out", e);
                }
            }
        } while (retry);

        return rcv;
    }

    public byte[] get(String key) throws IOException {
        Receiver rcv = sendCmd(new ValueReceiver(), "get", key, null, null);
        return ((ValueReceiver) rcv).getValue();
    }

    public String getstr(String key) throws IOException {
        Receiver rcv = sendCmd(new ValueReceiver(), "get", key, null, null);
        return ((ValueReceiver) rcv).getValue().toString();
    }

    private boolean set(String cmd, String key, byte[] value, int expt)
            throws IOException {
        Receiver rcv = sendCmd(new StringReceiver(), cmd, key, "0 " + expt
                + " " + value.length, value);
        return rcv.toString().equals("STORED");
    }

    public boolean set(String key, byte[] value, int expt)
            throws IOException {
        return set("set", key, value, expt);
    }
    
    public boolean set(String key, String value, int expt)
        throws IOException {
        return set("set", key, value.getBytes(), expt);        
    }

    public boolean add(String key, byte[] value, int expt)
            throws IOException {
        return set("add", key, value, expt);
    }

    public boolean add(String key, String value, int expt)
            throws IOException {
        return set("add", key, value.getBytes(), expt);
    }

    public boolean replace(String key, byte[] value, int expt)
            throws IOException {
        return set("replace", key, value, expt);
    }

    public boolean replace(String key, String value, int expt)
            throws IOException {
        return set("replace", key, value.getBytes(), expt);
    }

    public boolean append(String key, byte[] value, int expt)
            throws IOException {
        return set("append", key, value, expt);
    }

    public boolean append(String key, String value, int expt)
            throws IOException {
        return set("append", key, value.getBytes(), expt);
    }

    public boolean prepend(String key, byte[] value, int expt)
            throws IOException {
        return set("prepend", key, value, expt);
    }

    public boolean prepend(String key, String value, int expt)
            throws IOException {
        return set("prepend", key, value.getBytes(), expt);
    }

    public boolean incr(String key, int value) throws IOException {
        Receiver rcv = sendCmd(new StringReceiver(), "incr", key, "" + value,
                null);
        return rcv.toString().equals("STORED");
    }

    public boolean decr(String key, int value) throws IOException {
        Receiver rcv = sendCmd(new StringReceiver(), "decr", key, "" + value,
                null);
        return rcv.toString().equals("STORED");
    }

    public boolean delete(String key) throws IOException {
        Receiver rcv = sendCmd(new StringReceiver(), "delete", key, null, null);
        return rcv.toString().equals("DELETED");
    }

    public boolean setExpt(String key, int expt) throws IOException {
        Receiver rcv = sendCmd(new StringReceiver(), "set_expt", key,
                "" + expt, null);
        return rcv.toString().equals("STORED");
    }

    public boolean cas(String key, int expt, Cas callback)
            throws IOException {
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
