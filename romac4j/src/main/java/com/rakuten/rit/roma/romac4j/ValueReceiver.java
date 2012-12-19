package com.rakuten.rit.roma.romac4j;

import java.util.concurrent.TimeoutException;

import com.rakuten.rit.roma.romac4j.pool.Connection;

public class ValueReceiver extends Receiver {
    byte[] value;
    
    @Override
    void receive(Connection con) throws TimeoutException {
        // TODO Auto-generated method stub

    }

    byte[] getValue(){
        // TODO
        return value;
    }
    
    int getCasid(){
        // TODO
        return -1;
    }
    
}
