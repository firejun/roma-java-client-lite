package com.rakuten.rit.roma.romac4j;

import java.util.concurrent.TimeoutException;

import com.rakuten.rit.roma.romac4j.pool.Connection;

public abstract class Receiver {
    public int retry = 0;
    public abstract void receive(Connection con) throws TimeoutException;
}
