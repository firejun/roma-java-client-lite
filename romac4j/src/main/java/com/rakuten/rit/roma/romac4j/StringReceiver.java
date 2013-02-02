package com.rakuten.rit.roma.romac4j;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rakuten.rit.roma.romac4j.connection.Connection;

public class StringReceiver extends Receiver {

    String str = null;

    @Override
    public void receive(Connection con) throws TimeoutException, IOException {
        str = con.readLine();
    }

    public String toString() {
        return str;
    }
    
    public boolean isStroed() {
        return str.equals("STORED");
    }
    
    public boolean isDeleted() {
        return str.equals("DELETED");
    }

}
