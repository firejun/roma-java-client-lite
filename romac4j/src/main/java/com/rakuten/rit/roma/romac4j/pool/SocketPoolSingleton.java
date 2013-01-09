package com.rakuten.rit.roma.romac4j.pool;

import java.io.IOException;
import java.net.ConnectException;
import java.util.HashMap;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

public class SocketPoolSingleton {
    protected static Logger log = Logger.getLogger(SocketPoolSingleton.class
            .getName());
    private static SocketPoolSingleton instance = new SocketPoolSingleton();

    private SocketPoolSingleton() {
    }

    public static SocketPoolSingleton getInstance() {
        return instance;
    }

    private GenericObjectPool.Config config;
    private HashMap<String, GenericObjectPool<Connection>> poolMap;
    private int timeout;
    private int expTimeout;
    private int bufferSize;

    public void setEnv(int maxActive, int maxIdle, int timeout, int expTimeout,
            int bufferSize) {
        poolMap = new HashMap<String, GenericObjectPool<Connection>>();
        config = new GenericObjectPool.Config();
        config.maxActive = maxActive;
        config.maxIdle = maxIdle;
        config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
        //config.testOnBorrow = true;
        this.timeout = timeout;
        this.expTimeout = expTimeout;
        this.bufferSize = bufferSize;
    }

    public synchronized Connection getConnection(String nodeId) throws Exception {
        GenericObjectPool<Connection> pool = null;
        Connection con = null;
        String[] host = nodeId.split("_");
        if (poolMap != null && poolMap.containsKey(nodeId)) {
            pool = poolMap.get(nodeId);
        } else {
            PoolableObjectFactory<Connection> factory = new SocketPoolFactory(
                    host[0], Integer.valueOf(host[1]));
            pool = new GenericObjectPool<Connection>(factory, config);
            poolMap.put(nodeId, pool);
        }
        con = pool.borrowObject();
        con.setNodeId(nodeId);
        con.setSoTimeout(timeout);
        con.setBufferSize(bufferSize);

        return con;
    }

    public synchronized void deleteConnection(String nodeId) {
        GenericObjectPool<Connection> pool = null;
        if (poolMap.containsKey(nodeId)) {
            pool = poolMap.get(nodeId);
            pool.clear();
        }
        poolMap.remove(nodeId);
    }

    public synchronized void returnConnection(Connection con) {
        GenericObjectPool<Connection> pool = null;
        if (poolMap.containsKey(con.getNodeId())) {
            pool = poolMap.get(con.getNodeId());
            try {
                con.setSoTimeout(expTimeout);
                pool.returnObject(con);
                poolMap.put(con.getNodeId(), pool);
            } catch (Exception e) {
                System.out.println("Can't return the Socket.");
                e.printStackTrace();
            }
        }
    }
}
