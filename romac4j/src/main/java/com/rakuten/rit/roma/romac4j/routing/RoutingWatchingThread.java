package com.rakuten.rit.roma.romac4j.routing;

import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;

public final class RoutingWatchingThread extends Thread {
	protected static Logger log = Logger.getLogger(RoutingWatchingThread.class.getName());
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private Properties props;
	private Routing routing;
	private HashMap<String, Object> routingDump;
	private String mklHash;

	public RoutingWatchingThread(HashMap<String, Object> routingDump, String mklHash, Properties props) {
		this.props = props;
		this.routingDump = routingDump;
		this.mklHash = mklHash;
	}

	public void run() {
		sps = SocketPoolSingleton.getInstance();
		routing = new Routing(props);
		Random rnd = new Random(System.currentTimeMillis());
		Socket socket = null;
		String mklHash = null;
		String[] nodeId = null;
		while(true) {
			int rndVal = rnd.nextInt(new Integer(routingDump.get("numOfNodes").toString()));
			log.debug("rnd: " + rndVal);
			nodeId = (String[])routingDump.get("nodeId");
			try {
				socket = sps.getConnection(nodeId[rndVal]);
				if ((mklHash = routing.getMklHash(socket)) != null && !this.mklHash.equals(mklHash)) {
					this.mklHash = mklHash;
					routingDump = routing.getRoutingDump(socket);
					log.debug("Routing change!");
				} else {
					log.debug("Routing no change!");
				}
				sps.returnConnection(nodeId[rndVal], socket);
			} catch (Exception e) {
				log.debug("run() Error.");
				e.printStackTrace();
			}
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
		}
	}

	public synchronized HashMap<String, Object> getRoutingDump() {
		return routingDump;
	}
}
