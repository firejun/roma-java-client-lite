package com.rakuten.rit.roma.romac4j.pool;

import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

public class SocketPoolSingleton {
    protected static Logger log = Logger.getLogger(SocketPoolSingleton.class
            .getName());
    private static SocketPoolSingleton instance = new SocketPoolSingleton();

    private SocketPoolSingleton() {
        poolMap = Collections.synchronizedMap(new HashMap<String, GenericObjectPool<Connection>>());
    }

    public static SocketPoolSingleton getInstance() {
        return instance;
    }

    private GenericObjectPool.Config config;
    private Map<String, GenericObjectPool<Connection>> poolMap;
    private int timeout;
    private int bufferSize;

    public void setEnv(int maxActive, int maxIdle, int timeout, int bufferSize) {
        config = new GenericObjectPool.Config();
        config.maxActive = maxActive;
        config.maxIdle = maxIdle;
        config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
        //config.testOnBorrow = true;
        this.timeout = timeout;
        this.bufferSize = bufferSize;
    }

    public synchronized Connection getConnection(String nodeId) throws Exception {
        GenericObjectPool<Connection> pool = poolMap.get(nodeId);
        if (pool == null) {
            PoolableObjectFactory<Connection> factory = 
                    new SocketPoolFactory(nodeId, bufferSize);
            pool = new GenericObjectPool<Connection>(factory, config);
            poolMap.put(nodeId, pool);
        }
        Connection con = pool.borrowObject();
        con.setSoTimeout(timeout);

        return con;
    }

    public void deleteConnection(String nodeId) {
        GenericObjectPool<Connection> pool = poolMap.get(nodeId);
        if (pool != null) {
            synchronized(pool) {
                pool.clear();
                poolMap.remove(nodeId);
            }
        }
    }

    public void returnConnection(Connection con) {
        try {
            con.setSoTimeout(0);
        } catch (SocketException e) {
            log.warn("returnConnection() : " + e.getMessage());
            con.forceClose();
            return;
        }
        
        GenericObjectPool<Connection> pool = poolMap.get(con.getNodeId());
        if (pool != null) {
            synchronized(pool) {
                if(poolMap.containsKey(con.getNodeId())) {
                    try {
                        pool.returnObject(con);
                        return;
                    } catch (Exception e) {
                        log.error("returnConnection() : " + e.getMessage());
                    }
                }
            }
        }
        log.info("returnConnection() : close connection");
        con.forceClose();
    }
}
