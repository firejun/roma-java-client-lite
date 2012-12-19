package com.rakuten.rit.roma.romac4j.pool;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;

public class SocketPoolSingleton {
	protected static Logger log = Logger.getLogger(SocketPoolSingleton.class.getName());
	private static SocketPoolSingleton instance = new SocketPoolSingleton();

	private SocketPoolSingleton() {
	}

	public static SocketPoolSingleton getInstance() {
		return instance;
	}

	private GenericObjectPool.Config config;
	private HashMap<String, GenericObjectPool<Connection>> poolMap;
	private int numOfConnection;
	private int timeout;
	private int expTimeout;

	public void setEnv(int maxActive, int maxIdle, int timeout, int expTimeout ,int numOfConnection){
		poolMap = new HashMap<String, GenericObjectPool<Connection>>();
		config = new GenericObjectPool.Config();
		config.maxActive = maxActive;
		config.maxIdle = maxIdle;
		config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_GROW;
		this.numOfConnection = numOfConnection;
		this.timeout = timeout;
		this.expTimeout = expTimeout;
	}

	public synchronized Connection getConnection(String nodeId) {
		GenericObjectPool<Connection> pool = null;
		Connection con = null;
//		byte[] b = new byte[1];
		String[] host = nodeId.split("_");
		if (poolMap != null && poolMap.containsKey(nodeId)) {
//			con = new Connection();
//			try {
//				socket.connect(new InetSocketAddress(host[0], Integer.valueOf(host[1])));
//				BufferedInputStream is = new BufferedInputStream(socket.getInputStream());
//				is.read(b, 0, 0);
//				log.debug(b);
//			} catch (Exception e) {
//				e.printStackTrace();
//			}

			pool = poolMap.get(nodeId);
		} else {
//			String[] host = nodeId.split("_");
			PoolableObjectFactory<Connection> factory =
				new SocketPoolFactory(host[0], Integer.valueOf(host[1]));
			pool = new GenericObjectPool<Connection>(factory, config);
			//pool.setTestOnBorrow(true);
//			try {
//				for (int i=0; i < numOfConnection;i++) {
//					pool.addObject();					
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
			poolMap.put(nodeId, pool);
		}

		try {
//			boolean boo = pool.getTestOnBorrow();
//			log.debug("boo: " + boo);

			socket = pool.borrowObject();
			socket.setSoTimeout(timeout);

		} catch (Exception e) {
			try {
				socket.close();
				poolMap.remove(nodeId);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}
		
		return socket;

	}

//	public synchronized void deleteConnection(String nodeId) {
//
//	}

	public synchronized void returnConnection(Connection con) {
		GenericObjectPool<Connection> pool = null;
		if (poolMap.containsKey(con.getNodeId())) {
			pool = poolMap.get(con.getNodeId());
			try {
				con.setSoTimeout(expTimeout);
				pool.returnObject(con);
				poolMap.put(nodeId, pool);
			} catch (Exception e) {
				System.out.println("Can't return the Socket.");
				e.printStackTrace();
			}
		}
	}
}
