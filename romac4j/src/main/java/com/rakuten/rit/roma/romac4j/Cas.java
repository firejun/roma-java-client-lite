package com.rakuten.rit.roma.romac4j;

public abstract class Cas {
    Object arg;
    public Cas(Object _arg){
        arg = _arg;
    }
    abstract public byte[] cas(ValueReceiver rcv);
}
