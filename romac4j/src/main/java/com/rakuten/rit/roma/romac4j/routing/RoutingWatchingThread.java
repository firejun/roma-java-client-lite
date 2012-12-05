package com.rakuten.rit.roma.romac4j.routing;

import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;

public final class RoutingWatchingThread extends Thread {
	protected static Logger log = Logger.getLogger(RoutingWatchingThread.class
			.getName());
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private Properties props;
	private Routing routing;
	private String mklHash;
	private RoutingData routingData;

	public RoutingWatchingThread(RoutingData routingData, String mklHash,
			Properties props) {
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
		while (true) {
			rndVal = rnd.nextInt(routingData.getNumOfNodes());
			log.debug("rnd: " + rndVal);
			nodeId = routingData.getNodeId();
			try {
				socket = sps.getConnection(nodeId[rndVal]);
				if ((mklHash = routing.getMklHash(socket)) != null
						&& !this.mklHash.equals(mklHash)) {
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

	public long getVn(String key)
			throws NoSuchAlgorithmException {
		int divBits = 0;
		int dgstBits = 0;
		synchronized (routingData) {
			divBits = routingData.getDivBits();
			dgstBits = routingData.getDgstBits();
		}
		long mask = ((1L << divBits) - 1) << (dgstBits - divBits);
		MessageDigest md = MessageDigest.getInstance("SHA1");
		md.update(key.getBytes());
		byte[] b = md.digest();
		long h = ((long) b[b.length - 7] << 48) & 0xff000000000000L
				| ((long) b[b.length - 6] << 40) & 0xff0000000000L
				| ((long) b[b.length - 5] << 32) & 0xff00000000L
				| ((long) b[b.length - 4] << 24) & 0xff000000L
				| ((long) b[b.length - 3] << 16) & 0xff0000L
				| ((long) b[b.length - 2] << 8) & 0xff00L
				| (long) b[b.length - 1] & 0xffL;
		long i = (h & mask) >> (dgstBits - divBits);
		return i;
	}

	public RoutingData getRoutingData() {
		synchronized (routingData) {
			return routingData;
		}
	}
}
