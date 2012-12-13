package com.rakuten.rit.roma.romac4j;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.commands.BasicCommands;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
import com.rakuten.rit.roma.romac4j.routing.RoutingData;
import com.rakuten.rit.roma.romac4j.routing.RoutingWatchingThread;
import com.rakuten.rit.roma.romac4j.utils.Constants;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient {
    protected static Logger log = Logger.getLogger(RomaClient.class.getName());
    private PropertiesUtils pu = new PropertiesUtils();
    private Properties props;
    private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
    private RoutingWatchingThread rwt;
    private BasicCommands basicCommands = new BasicCommands();

    public RomaClient() {
        BasicConfigurator.configure();
        log.debug("Init Section.");

        try {
            // Set properties values
            props = pu.getRomaClientProperties();
            setEnv();

            Socket socket = sps
                    .getConnection(props.getProperty("address_port"));
            Routing routing = new Routing(props);
            String mklHash = routing.getMklHash(socket);
            RoutingData routingData = routing.getRoutingDump(socket);
            log.debug("Init mklHash: " + mklHash);
            sps.returnConnection(props.getProperty("address_port"), socket);
            rwt = new RoutingWatchingThread(routingData, mklHash, props);
            rwt.start();

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

    public void close() {
        rwt.setStatus(true);
    }

    protected Receiver sendCmd(Receiver rcv, String cmd, String key,
            String opt, byte[] value) {
        boolean retry;
        do {
            retry = false;
            Connection con = routing.getConnection(key);
            try {
                con.write(cmd, key, opt, value);
                rcv.receive(con);
                routing.returnConnection(con);
            } catch (TimeOutException e) {
                retry = true;
                routing.failCount();
                if (rcv.retry++ > 5) {
                    throw new Exception();
                }
            }
        } while (retry);

        return rcv;
    }

    public boolean set(String key, byte[] value, int expt) {
        Receiver rcv = sendCmd(new StringReceiver(), "set", key, value, "0 0 "
                + value.length);
        return rcv.toString().equals("STORED");
    }

    public boolean add(String key, byte[] value, int expt) {
        Receiver rcv = sendCmd(new StringReceiver(), "add", key, value, "0 0 "
                + value.length);
        return rcv.toString().equals("STORED");
    }

    public boolean replace(String key, byte[] value, int expt) {
        Receiver rcv = sendCmd(new StringReceiver(), "replace", key, value, "0 0 "
                + value.length);
        return rcv.toString().equals("STORED");
    }

    public boolean append(String key, byte[] value, int expt) {
        Receiver rcv = sendCmd(new StringReceiver(), "append", key, value, "0 0 "
                + value.length);
        return rcv.toString().equals("STORED");
    }

    public boolean prepend(String key, byte[] value, int expt) {
        Receiver rcv = sendCmd(new StringReceiver(), "prepend", key, value, "0 0 "
                + value.length);
        return rcv.toString().equals("STORED");
    }

    public boolean delete(String key) {
        Receiver rcv = sendCmd(new StringReceiver(), "delete", key);
        return rcv.toString().equals("DELETED");
    }

    public boolean cas(String key) {
        // TODO:cas-id?
        sendCmd("cas", key);
        String res = getResult();

        return true;
    }

    public byte[] get(String key) {
        Receiver rcv = sendCmd(new ValueReceiver(), "get", key);
        return ((ValueReceiver) rcv).value;
    }

//    public byte[] get(String key) {
//        byte[] result = null;
//
//        try {
//            String[] nodeId = rwt.getRoutingData().getNodeId();
//            long vn = rwt.getVn(key);
//            int[] arrVn = rwt.getRoutingData().getVNode().get(vn);
//            log.debug("vn: " + vn + " nodeId: " + nodeId[arrVn[0]]);
//            Socket socket = sps.getConnection(nodeId[arrVn[0]]);
//            result = basicCommands.get(key, socket, props);
//            sps.returnConnection(nodeId[arrVn[0]], socket);
//        } catch (Exception e) {
//            log.error("Get failed.");
//        }
//        return result;
//    }
}
