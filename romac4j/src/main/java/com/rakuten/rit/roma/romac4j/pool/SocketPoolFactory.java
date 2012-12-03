package com.rakuten.rit.roma.romac4j.pool;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.pool.PoolableObjectFactory;

public class SocketPoolFactory implements PoolableObjectFactory<Socket> {
	private String host;
	private int port;
	
	public SocketPoolFactory(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public Socket makeObject() throws IOException {
		Socket socket = new Socket();
		socket.connect(new InetSocketAddress(host, port));
		return socket;	
	}

	public void destroyObject(Socket socket) throws Exception {
		if (socket instanceof Socket) {
			socket.close();
		}		
	}

	public boolean validateObject(Socket socket) {
		return socket.isConnected();
//		byte[] b = new byte[1];
//		if (socket instanceof Socket) {
//			try {
//				BufferedInputStream is = new BufferedInputStream(socket.getInputStream());
//				is.read(b, 0, 0);
//			} catch (IOException e) {
//				e.printStackTrace();
//				return false;
//			} 
//			if (b[0] == 0) {
//				return true;
//			}
//			return true;
//		}
//		return false;
	}

	public void activateObject(Socket socket) throws Exception {
		
	}

	public void passivateObject(Socket obj) throws Exception {
		
	}
}
