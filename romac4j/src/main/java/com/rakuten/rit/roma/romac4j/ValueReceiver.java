package com.rakuten.rit.roma.romac4j;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.connection.Connection;

public class ValueReceiver extends Receiver {
    protected static Logger log = Logger.getLogger(ValueReceiver.class.getName());
    private ArrayList<String> headers = new ArrayList<String>();
    private ArrayList<byte[]> values = new ArrayList<byte[]>();
    
    private boolean receiveValue(Connection con) throws IOException, ParseException {
        String str = con.readLine();
        if(str == null){
            log.error("receiveValue() : first line is null.");
            throw new IOException("receiveValue() : first line is null.");
        }
        if(str.equals("END")) return false;

        headers.add(str);
        int len = 0;
        try {
            String[] header = str.split(" ");
            if (header.length >= 4) {
                len = Integer.valueOf(header[3]);
            } else {
                log.error("receive() : header format error [" + str + "]");
                throw new ParseException(str, -1);
            }
        } catch (NumberFormatException e) {
            log.error("receive() : NumberFormatException [" + str + "] " + e.getMessage());
            throw new ParseException(str, -1);
        }
        
        if (len > 0) {
            values.add(con.readValue(len));
        } else {
            values.add(new byte[0]);
        }
        return true;
    }
    
    @Override
    public void receive(Connection con) throws TimeoutException, IOException, ParseException {
        while(receiveValue(con));
    }
    
    public int size() {
        return values.size();
    }
    
    public byte[] getValue() {
        return getValue(0);
    }

    public byte[] getValue(int n) {
        if(values.size() > n) return values.get(n);
        return null;
    }

    public String getValueString() {
        return getValueString(0);
    }
 
    public String getValueString(int n) {
        byte[] v = getValue(n);
        if(v == null) return null;
        return new String(v);
    }

    public String getHeader() {
        return getHeader(0);
    }
    
    public String getHeader(int n) {
        if(headers.size() > n) return headers.get(n);
        return null;
    }

    public int getCasid() throws ParseException {
        String str = getHeader();
        if(str == null){
            log.warn("getCasid() : first line is null.");
            throw new RuntimeException("can't get <cas unique>.");
        }
        String[] header = str.split(" ");
        int len = 0;
        if (header.length == 5) {
            try{
                len = Integer.valueOf(header[4]);
            }catch(NumberFormatException e){
                log.warn("getCasid() : [" + str + "] " + e.getMessage());
                throw new ParseException(str, 4);
            }
        }else{
            log.warn("getCasid() : [" + str + "] can't get <cas unique>.");
            throw new ParseException(str, -1);
        }
        return len;
    }
}
