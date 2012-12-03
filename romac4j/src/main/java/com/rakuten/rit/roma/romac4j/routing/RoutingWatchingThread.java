package com.rakuten.rit.roma.romac4j.routing;

import java.net.Socket;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;

public final class RoutingWatchingThread extends Thread {
	protected static Logger log = Logger.getLogger(RoutingWatchingThread.class.getName());
	private HashMap<String, Object> routingDump;
	private String mklHash;

	public RoutingWatchingThread(HashMap<String, Object> routingDump, String mklHash) {
		this.routingDump = routingDump;
		this.mklHash = mklHash;
	}

	public void run() {
		SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
		Random rnd = new Random(new Integer(routingDump.get("numOfNodes").toString()));
		while(true) {
			int rndVal = rnd.nextInt();
			log.debug("rnd: " + rndVal);
			String[] nodeId = (String[])routingDump.get("nodeId");
			Socket socket = null;
			try {
				socket = sps.getConnection(nodeId[rndVal]);
				String mklHash = Routing.getMklHash(socket);
				if (!this.mklHash.equals(mklHash)) {
					this.mklHash = mklHash;
					try {
						log.debug("Routing change!");
						routingDump = Routing.getRoutingDump(socket);
					} catch (Exception e) {
						e.printStackTrace();
				}
				} else {
					log.debug("Routing no change!");
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			try {
				sps.returnConnection(nodeId[rndVal], socket);
				Thread.sleep(5000);
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
	}
}
