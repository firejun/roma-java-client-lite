package com.rakuten.rit.roma.romac4j.pool;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.pool.PoolableObjectFactory;

public class SocketPoolFactory implements PoolableObjectFactory<Connection> {
	private String host;
	private int port;
	
	public SocketPoolFactory(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public Connection makeObject() throws IOException {
	    Connection con = new Connection();
		con.connect(new InetSocketAddress(host, port));
		return con;	
	}

	public void destroyObject(Connection con) throws Exception {
		if (con instanceof Connection) {
		    con.close();
		}		
	}

	public boolean validateObject(Connection con) {
		return con.isConnected();
	}

	public void activateObject(Connection con) throws Exception {
		
	}

	public void passivateObject(Connection con) throws Exception {
		
	}
}
