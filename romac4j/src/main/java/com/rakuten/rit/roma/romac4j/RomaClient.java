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
import com.rakuten.rit.roma.romac4j.utils.StringUtils;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient {
	protected static Logger log = Logger.getLogger(RomaClient.class.getName());
	private PropertiesUtils props = PropertiesUtils.getInstance();
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private HashMap<String, Object> routingDump;
	private String mklHash;
	private RoutingWatchingThread rwt;
	
	public RomaClient(){
		BasicConfigurator.configure();
		props.preparateProperties();
		log.debug("Init.");
		
		try {
			// Set properties values
			setEnv();

			Socket socket = sps.getConnection(props.getProperties().getProperty("address_port"));
			routingDump = Routing.getRoutingDump(socket);
			mklHash = Routing.getMklHash(socket);
			sps.returnConnection(props.getProperties().getProperty("address_port"), socket);

			rwt = new RoutingWatchingThread(routingDump, mklHash);
			rwt.start();

			while(true) {
				try {
					routingDump = rwt.getRoutingDump();
					Thread.sleep(5000);
				} catch (Exception e) {
					log.error("Main Loop Error.");
				}
			}

		} catch (Exception e) {
			log.error("Main Error.");
		}
	}

	public void setEnv() {
		sps.setEnv(Integer.valueOf(props.getProperties().getProperty("maxActive")),
				Integer.valueOf(props.getProperties().getProperty("maxIdle")),
				Integer.valueOf(props.getProperties().getProperty("timeout")),
				Integer.valueOf(props.getProperties().getProperty("expTimeout")),
				Integer.valueOf(props.getProperties().getProperty("numOfConnection")));
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
		int rndVal = rnd.nextInt(new Integer(routingDump.get("numOfNodes").toString()));
		log.debug("rnd: " + rndVal);
		String[] nodeId = (String[])routingDump.get("nodeId");
		try {
			socket = sps.getConnection(nodeId[rndVal]);
			//socket = sps.getConnection(StringUtils.calcVn(key));
			b = BasicCommands.get(key, socket);
			sps.returnConnection(StringUtils.calcVn(key), socket);
		} catch (Exception e) {
			log.debug("Error");
		}
		return b;
	}
}
