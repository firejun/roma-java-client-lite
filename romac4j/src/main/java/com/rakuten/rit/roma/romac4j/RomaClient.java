package com.rakuten.rit.roma.romac4j;

import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

public class RomaClient extends ClientObject {
    protected static Logger log = Logger.getLogger(RomaClient.class.getName());

    public RomaClient(ClientObject obj) {
        super(obj);
    }

    public RomaClient(String nodeId) {
        super(nodeId);
    }

    public byte[] get(String key) throws IOException {
        Receiver rcv = sendCmd(new ValueReceiver(), "get", key, null, null);
        return ((ValueReceiver) rcv).getValue();
    }

    public String getString(String key) throws IOException {
        Receiver rcv = sendCmd(new ValueReceiver(), "get", key, null, null);
        return ((ValueReceiver) rcv).getValueString();
    }

    public byte[][] gets(String[] keys) throws IOException {
        return null;
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
