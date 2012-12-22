package com.rakuten.rit.roma.romac4j;

import java.util.concurrent.TimeoutException;

import com.rakuten.rit.roma.romac4j.pool.Connection;

public class ValueReceiver extends Receiver {
    byte[] value;
    
    @Override
    public void receive(Connection con) throws TimeoutException {
        // TODO

    }

    public byte[] getValue(){
        // TODO

        return value;
    }

    public byte[] getRouting(){
        // TODO
        return value;
    }

    public int getCasid(){
        // TODO
        return -1;
    }
    
}
