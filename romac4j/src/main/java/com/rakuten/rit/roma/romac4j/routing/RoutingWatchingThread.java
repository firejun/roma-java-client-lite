package com.rakuten.rit.roma.romac4j.routing;

import java.net.Socket;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;

public final class RoutingWatchingThread extends Thread {
	protected static Logger log = Logger.getLogger(RoutingWatchingThread.class.getName());
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private Properties props;
	private Routing routing;
	private String mklHash;
	private RoutingData routingData;

	public RoutingWatchingThread(RoutingData routingData, String mklHash, Properties props) {
		this.props = props;
		this.routingData = routingData;
		this.mklHash = mklHash;
	}

	public void run() {
		sps = SocketPoolSingleton.getInstance();
		routing = new Routing(props);
		Random rnd = new Random(System.currentTimeMillis());
		Socket socket = null;
		String mklHash = null;
		String[] nodeId = null;
		int rndVal = 0;
		while(true) {
			rndVal = rnd.nextInt(routingData.getNumOfNodes());
			log.debug("rnd: " + rndVal);
			nodeId = routingData.getNodeId();
			try {
				socket = sps.getConnection(nodeId[rndVal]);
				if ((mklHash = routing.getMklHash(socket)) != null && !this.mklHash.equals(mklHash)) {
					this.mklHash = mklHash;
					RoutingData tempBuff = routing.getRoutingDump(socket);
					synchronized (routingData) {
						routingData = tempBuff;
					}
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

	public RoutingData getRoutingData() {
		synchronized (routingData){
			return routingData;
		}
	}
}
