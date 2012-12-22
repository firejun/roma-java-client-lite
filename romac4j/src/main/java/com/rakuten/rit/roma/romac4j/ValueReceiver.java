package com.rakuten.rit.roma.romac4j;

import java.util.concurrent.TimeoutException;

import com.rakuten.rit.roma.romac4j.pool.Connection;

public class ValueReceiver extends Receiver {
    byte[] value;

    @Override
    public void receive(Connection con) throws TimeoutException {
        value = con.readValue();
    }

    public byte[] getValue() {
        return value;
    }

    public int getCasid() {
        return -1;
    }
}
