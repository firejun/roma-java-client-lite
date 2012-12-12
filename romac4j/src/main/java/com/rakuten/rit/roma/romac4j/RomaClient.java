package com.rakuten.rit.roma.romac4j;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.commands.BasicCommands;
import com.rakuten.rit.roma.romac4j.pool.SocketPoolSingleton;
import com.rakuten.rit.roma.romac4j.routing.Routing;
import com.rakuten.rit.roma.romac4j.routing.RoutingData;
import com.rakuten.rit.roma.romac4j.routing.RoutingWatchingThread;
import com.rakuten.rit.roma.romac4j.utils.Constants;
import com.rakuten.rit.roma.romac4j.utils.PropertiesUtils;

public class RomaClient {
	protected static Logger log = Logger.getLogger(RomaClient.class.getName());
	private PropertiesUtils pu = new PropertiesUtils();
	private Properties props;
	private SocketPoolSingleton sps = SocketPoolSingleton.getInstance();
	private RoutingWatchingThread rwt;
	private BasicCommands basicCommands = new BasicCommands();

	public RomaClient() {
		BasicConfigurator.configure();
		log.debug("Init Section.");

		try {
			// Set properties values
			props = pu.getRomaClientProperties();
			setEnv();

			Socket socket = sps
					.getConnection(props.getProperty("address_port"));
			Routing routing = new Routing(props);
			String mklHash = routing.getMklHash(socket);
			RoutingData routingData = routing.getRoutingDump(socket);
			log.debug("Init mklHash: " + mklHash);
			sps.returnConnection(props.getProperty("address_port"), socket);
			rwt = new RoutingWatchingThread(routingData, mklHash, props);
			rwt.start();

		} catch (Exception e) {
			log.error("Main Error.");
		}
	}

	public void setEnv() {
		sps.setEnv(Integer.valueOf(props.getProperty("maxActive")),
				Integer.valueOf(props.getProperty("maxIdle")),
				Integer.valueOf(props.getProperty("timeout")),
				Integer.valueOf(props.getProperty("expTimeout")),
				Integer.valueOf(props.getProperty("numOfConnection")));
	}

	public void setTimeout(int timeout) {
		props.setProperty("timeout", String.valueOf(timeout));
	}

	public void close() {
		rwt.setStatus(true);
	}
	
	public void sendCmd(String cmd, String ket){
		sendCmd(cmd, ket, null);
	}

	protected PrintWriter sendCmd(String cmd, String key, String opt){
		Context ctx = getPrimary();
		return sendCmd(ctx, cmd, key, opt);
	}

	protected PrintWriter sendCmd(Context ctx,String cmd, String key, String opt){
		PrintWriter writer = getWriter();
		try{
			writer.write(cmd + " " + key + " " + opt + Constants.CRLF);
			writer.flush();
		}catch(TimeOutException e){
			if(ctx.retry > 5){
				throw new Exception();
			}else{
				ctx = getNext();
				ctx.retry ++;
				sendCmd(ctx, cmd, key, opt);
			}
		}
		return writer;
	}

	protected String getResult(PrintWriter ctx){
		
		// readLine ‚ÌŽÀ‘•
		
		return null;
	}

	protected byte[] getValue(PrintWriter ctx){
		ValueRes vr = getValueRes(ctx);
		
		// 
		
		return null;
	}
	
	public boolean set(String key, byte[] value, int expt){
		
		String res = getResult(sendCmd("set ", key, "0 0 " + value.length));
	
		return true;
	}
	
	public boolean delete(String key){
		return getResult(sendCmd("delete ", key));
	}
	
	public boolean cas(String key){
		
		sendCmd("cas ", key);
		String res = getResult();
	
		return true;
	}
	
	public byte[] get2(String key) {
		
		byte[] value = getValue(sendCmd("get ", key));
		
		return null;
	}
	/**
	 * Get Command
	 * 
	 * @param key
	 * @return result
	 */
	public byte[] get(String key) {
		byte[] result = null;

		try {
			String[] nodeId = rwt.getRoutingData().getNodeId();
			long vn = rwt.getVn(key);
			int[] arrVn = rwt.getRoutingData().getVNode().get(vn);
			log.debug("vn: " + vn + " nodeId: " + nodeId[arrVn[0]]);
			Socket socket = sps.getConnection(nodeId[arrVn[0]]);
			result = basicCommands.get(key, socket, props);
			sps.returnConnection(nodeId[arrVn[0]], socket);
		} catch (Exception e) {
			log.error("Get failed.");
		}
		return result;
	}
}
