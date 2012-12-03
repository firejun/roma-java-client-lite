package com.rakuten.rit.roma.romac4j;

import java.net.Socket;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.commands.BasicCommands;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
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
			Routing routing = new Routing(props.getRomaClientProperties());
			Socket socket = sps.getConnection(props.getRomaClientProperties().getProperty("address_port"));
			HashMap<String, Object> routingDump = routing.getRoutingDump(socket);
			String mklHash = routing.getMklHash(socket);
			sps.returnConnection(props.getRomaClientProperties().getProperty("address_port"), socket);

			rwt = new RoutingWatchingThread(routingDump, mklHash, props.getRomaClientProperties());
			rwt.start();

//			while(true) {
//				try {
//					routingDump = rwt.getRoutingDump();
//					Thread.sleep(5000);
//				} catch (Exception e) {
//					log.error("Main Loop Error.");
//				}
//			}

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
		byte[] b = null;
		Random rnd = new Random(System.currentTimeMillis());
		int rndVal = rnd.nextInt(new Integer(rwt.getRoutingDump().get("numOfNodes").toString()));

		String[] nodeId = (String[])rwt.getRoutingDump().get("nodeId");
		log.debug("Access NodeId: " + nodeId[rndVal]);
		try {
			socket = sps.getConnection(nodeId[rndVal]);
			b = BasicCommands.get(key, socket, props.getRomaClientProperties());
			sps.returnConnection(nodeId[rndVal], socket);
		} catch (Exception e) {
			log.debug("Error");
		}
		return b;
	}
}
