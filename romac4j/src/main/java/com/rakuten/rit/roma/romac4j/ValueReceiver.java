package com.rakuten.rit.roma.romac4j;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.connection.Connection;

public class ValueReceiver extends Receiver {
    protected static Logger log = Logger.getLogger(ValueReceiver.class
            .getName());
    private String str = null;
    private byte[] value = null;

    @Override
    public void receive(Connection con) throws TimeoutException, IOException, ParseException {
        int len = 0;
        str = con.readLine();
        if(str == null){
            log.error("receive() : first line is null.");
            throw new IOException("receive() : first line is null.");
        }
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
            value = con.readValue(len);
            con.readLine(); // "END\r\n"
        } else {
            value = new byte[0];
        }
    }

    public byte[] getValue() {
        return value;
    }

    public String getValueString() {
        return new String(value);
    }
    
    public int getCasid() throws ParseException {
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
