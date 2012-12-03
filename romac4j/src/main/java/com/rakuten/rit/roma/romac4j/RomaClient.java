package com.rakuten.rit.roma.romac4j;

import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.commands.BasicCommands;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
import com.rakuten.rit.roma.romac4j.routing.RoutingWatchingThread;
import com.rakuten.rit.roma.romac4j.utils.StringUtils;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient extends Thread {
	protected static Logger log = Logger.getLogger(RomaClient.class.getName());
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private HashMap<String, Object> routingDump;
	private String mklHash;
	
	public RomaClient(){
		BasicConfigurator.configure();
		log.debug("Init.");
		Properties props = null;
		props = PropertiesUtils.preparateProperties();
		RoutingWatchingThread rwt;
		try {
			// Set properties values
			sps.setEnv(Integer.valueOf(props.getProperty("maxActive")),
					Integer.valueOf(props.getProperty("maxIdle")),
					Integer.valueOf(props.getProperty("timeout")),
					Integer.valueOf(props.getProperty("expTimeout")),
					Integer.valueOf(props.getProperty("numOfConnection")));

			Socket socket = sps.getConnection(props.getProperty("address_port"));
			routingDump = Routing.getRoutingDump(socket);
			mklHash = Routing.getMklHash(socket);
			sps.returnConnection(props.getProperty("address_port"), socket);

			rwt = new RoutingWatchingThread(routingDump, mklHash);

			// Thread start
			rwt.start();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @param String key
	 * @return byte[]
	 */
	public byte[] get(String key) {
		Socket socket;
		byte[] b = null;
		try {
			socket = sps.getConnection(StringUtils.calcVn(key));
			b = BasicCommands.get(key, socket);
			sps.returnConnection(StringUtils.calcVn(key), socket);
		} catch (Exception e) {
			log.debug("Error");
			e.printStackTrace();
		}
		return b;
	}
}
