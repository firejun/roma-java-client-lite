package com.rakuten.rit.roma.romac4j;

import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient {
    protected static Logger log = Logger.getLogger(RomaClient.class.getName());
    private PropertiesUtils pu = new PropertiesUtils();
    private Properties props;
    private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
    private Routing routing;
    //private GetRouting routing;
    //private BasicCommands basicCommands = new BasicCommands();

    private int maxRetry = 5;
    
    public RomaClient() {
        BasicConfigurator.configure();
        log.debug("Init Section.");

        try {
            // Set properties values
            props = pu.getRomaClientProperties();
            setEnv();

            //Socket socket = sps
            //        .getConnection(props.getProperty("address_port"));
            //GetRouting getRouting = new GetRouting(props);
            //String mklHash = getRouting.getMklHash(socket);
            //RoutingData routingData = getRouting.getRoutingDump(socket);
            //log.debug("Init mklHash: " + mklHash);
            //sps.returnConnection(props.getProperty("address_port"), socket);
            routing = new Routing(props);
            routing.start();

        } catch (Exception e) {
            log.error("Main Error.");
        }
    }

    public void setEnv() {
        sps.setEnv(Integer.valueOf(props.getProperty("maxActive")),
                Integer.valueOf(props.getProperty("maxIdle")),
                Integer.valueOf(props.getProperty("timeout")),
                Integer.valueOf(props.getProperty("expTimeout")),
                Integer.valueOf(props.getProperty("numOfConnection")));
    }

    public void setTimeout(int timeout) {
        props.setProperty("timeout", String.valueOf(timeout));
    }

    public void destroy() {
        routing.setStatus(true);
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
            Connection con = routing.getConnection(key);
            try {
                con.write(cmd, key, opt, value, casid);
                rcv.receive(con);
                routing.returnConnection(con);
            } catch (TimeoutException e) {
                retry = true;
                routing.failCount(con);
                if (rcv.retry++ > maxRetry) {
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

    private boolean set(String cmd, String key, byte[] value, int expt) throws RetryOutException{
        Receiver rcv = sendCmd(new StringReceiver(), cmd, key,
                "0 " + expt + " " + value.length, value);
        return rcv.toString().equals("STORED");        
    }
    
    public boolean set(String key, byte[] value, int expt) throws RetryOutException {
        return set("set", key, value, expt);
    }

    public boolean add(String key, byte[] value, int expt) throws RetryOutException {
        return set("add", key, value, expt);
    }

    public boolean replace(String key, byte[] value, int expt) throws RetryOutException {
        return set("replace", key, value, expt);
    }

    public boolean append(String key, byte[] value, int expt) throws RetryOutException {
        return set("append", key, value, expt);
    }

    public boolean prepend(String key, byte[] value, int expt) throws RetryOutException {
        return set("prepend", key, value, expt);
    }

    public boolean incr(String key, int value) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "incr", key, "" + value, null);
        return rcv.toString().equals("STORED");
    }

    public boolean decr(String key, int value) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "decr", key, "" + value, null);
        return rcv.toString().equals("STORED");
    }

    public boolean delete(String key) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "delete", key, null, null);
        return rcv.toString().equals("DELETED");
    }

    public boolean setExpt(String key, int expt) throws RetryOutException {
        Receiver rcv = sendCmd(new StringReceiver(), "set_expt", key, "" + expt, null);
        return rcv.toString().equals("STORED");
    }

    public boolean cas(String key, Cas callback) throws RetryOutException {
        Receiver rcv = sendCmd(new ValueReceiver(), "gets", key, null, null);
        byte[] value = callback.cas((ValueReceiver)rcv);
        
        Receiver rcv2 = sendCmd(new StringReceiver(), "cas", key, null, value, ((ValueReceiver)rcv).getCasid());
        return rcv2.toString().equals("STORED");
    }
    
    /*
     * res = rv.cas("key", new Cas(arg){
     *   cas(ValueReceiver rcv){
     *   
     *      return value;
     *   }
     *   };
     *   );
     */

    // public byte[] get(String key) {
    // byte[] result = null;
    //
    // try {
    // String[] nodeId = rwt.getRoutingData().getNodeId();
    // long vn = rwt.getVn(key);
    // int[] arrVn = rwt.getRoutingData().getVNode().get(vn);
    // log.debug("vn: " + vn + " nodeId: " + nodeId[arrVn[0]]);
    // Socket socket = sps.getConnection(nodeId[arrVn[0]]);
    // result = basicCommands.get(key, socket, props);
    // sps.returnConnection(nodeId[arrVn[0]], socket);
    // } catch (Exception e) {
    // log.error("Get failed.");
    // }
    // return result;
    // }
}
