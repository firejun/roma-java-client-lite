package com.rakuten.rit.roma.romac4j;

import java.text.ParseException;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient {
    protected static Logger log = Logger.getLogger(RomaClient.class.getName());
    private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
    private Routing routing;
    private int maxRetry;

    public RomaClient() {
        BasicConfigurator.configure();
        log.debug("Init Section.");

        try {
            // Set properties values
            Properties props = PropertiesUtils.getRomaClientProperties();
            sps.setEnv(Integer.parseInt(props.getProperty("maxActive")),
                    Integer.parseInt(props.getProperty("maxIdle")),
                    Integer.parseInt(props.getProperty("timeout")),
                    Integer.parseInt(props.getProperty("expTimeout")),
                    Integer.parseInt(props.getProperty("bufferSize")));
            maxRetry = Integer.parseInt(props.getProperty("maxRetry"));
            log.debug("maxRetry: " + maxRetry);

            routing = new Routing(props.getProperty("address_port"));
            routing.setFailCount(Integer.parseInt(props
                    .getProperty("failCount")));
            routing.setThreadSleep(Integer.parseInt(props
                    .getProperty("threadSleep")));
            routing.start();
            Thread.sleep(1000);
        } catch (Exception e) {
            log.error("Main Error.");
        }
    }

    public void destroy() {
        routing.stopThread();
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
