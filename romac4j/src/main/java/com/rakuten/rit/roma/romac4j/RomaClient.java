package com.rakuten.rit.roma.romac4j;

import java.net.Socket;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.commands.BasicCommands;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
import com.rakuten.rit.roma.romac4j.routing.RoutingData;
import com.rakuten.rit.roma.romac4j.routing.RoutingWatchingThread;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient {
	protected static Logger log = Logger.getLogger(RomaClient.class.getName());
	private PropertiesUtils props = new PropertiesUtils();
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private RoutingWatchingThread rwt;

	public RomaClient(){
		BasicConfigurator.configure();
		log.debug("Init Section.");
		
		try {
			// Set properties values
			setEnv();

			Socket socket = sps.getConnection(props.getRomaClientProperties().getProperty("address_port"));
			Routing routing = new Routing(props.getRomaClientProperties());
			String mklHash = routing.getMklHash(socket);
			RoutingData routingData = routing.getRoutingDump(socket);			
			log.debug("Init mklHash: " + mklHash);
			sps.returnConnection(props.getRomaClientProperties().getProperty("address_port"), socket);
			rwt = new RoutingWatchingThread(routingData, mklHash, props.getRomaClientProperties());
			rwt.start();

		} catch (Exception e) {
			log.error("Main Error.");
		}
	}

	public void setEnv() {
		sps.setEnv(Integer.valueOf(props.getRomaClientProperties().getProperty("maxActive")),
				Integer.valueOf(props.getRomaClientProperties().getProperty("maxIdle")),
				Integer.valueOf(props.getRomaClientProperties().getProperty("timeout")),
				Integer.valueOf(props.getRomaClientProperties().getProperty("expTimeout")),
				Integer.valueOf(props.getRomaClientProperties().getProperty("numOfConnection")));
	}

	public void setTimeout(int timeout) {
		props.setTimeout(timeout);		
	}

	/**
	 * 
	 * @param String key
	 * @return byte[]
	 */
	public byte[] get(String key) {
		Socket socket;
		byte[] result = null;

		String[] nodeId = rwt.getRoutingData().getNodeId();
		long vn;
		int[] i;
		try {
			vn = rwt.getVn(key);
			log.debug("vn: " + vn);
			i = rwt.getRoutingData().getVNode().get(vn);
			log.debug("nodeId: " + nodeId[i[0]]);
			socket = sps.getConnection(nodeId[i[0]]);
			result = BasicCommands.get(key, socket, props.getRomaClientProperties());
			sps.returnConnection(nodeId[i[0]], socket);
		} catch (NoSuchAlgorithmException e1) {
			log.error("NoSuchAlgorithmException Error.");
		} catch (Exception e) {
			log.error("Error");
		}
		return result;
	}
}
