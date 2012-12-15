package com.rakuten.rit.roma.romac4j.routing;

import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.pool.Connection;
import com.rakuten.rit.roma.romac4j.utils.Constants;
import com.rakuten.rit.roma.romac4j.utils.StringUtils;

public class Routing {
	protected static Logger log = Logger.getLogger(Routing.class.getName());
	private Properties props;

	public Routing(Properties props) {
		this.props = props;
	}

	public String getMklHash(Socket socket) {
		PrintWriter writer = null;
		BufferedInputStream is = null;
		String str = null;
		try {
			// Output stream open
			writer = new PrintWriter(socket.getOutputStream(), true);

			// Execute command
			writer.write("mklhash 0" + Constants.CRLF);
			writer.flush();

			// Receive header part
			is = new BufferedInputStream(socket.getInputStream());

			// # Length
			str = StringUtils.readOneLine(is,
					Integer.valueOf(props.getProperty("bufferSize")));
		} catch (Exception e) {
		}
		return str;
	}

	public RoutingData getRoutingDump(Socket socket) throws Exception {
		RoutingData routingData = new RoutingData();
		try {
			// Output stream open
			PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

			// Execute command
			writer.write("routingdump bin" + Constants.CRLF);
			writer.flush();

			// Receive header part
			BufferedInputStream is = new BufferedInputStream(socket.getInputStream());

			// # Length
			String str = StringUtils.readOneLine(is,
					Integer.valueOf(props.getProperty("bufferSize")));
			
			int rtLen = Integer.parseInt(str);
			log.debug(rtLen);

			// Initialize buffer
			byte[] b = new byte[Integer.valueOf(props.getProperty("bufferSize"))];
			byte[] buff = new byte[rtLen + Constants.CRLF_LEN];

			// Read from stream
			int receiveCount = 0;
			int count = 0;
			while (receiveCount < rtLen + Constants.CRLF_LEN) {
				count = is.read(b, 0, Integer.valueOf(props.getProperty("bufferSize")));
				System.arraycopy(b, 0, buff, receiveCount, count);
				receiveCount += count;
			}

			// # 2 bytes('RT'):magic code
			int pos = 0;
			str = new String(new byte[]{buff[pos], buff[pos + 1]});
			if (!str.equals("RT")) {
				log.debug("This is not RT Data.");
				throw new Exception("Illegal Format.");
			}
			pos += 2;

			// # unsigned short:format version
			int formatVer = (buff[pos] << 8) & 0xff00 | buff[pos + 1] & 0xff;
			pos += 2;
			log.debug("formatVer:" + formatVer);

			// # unsigned char:dgst_bits
			short dgstBits = buff[pos];
			pos += 1;
			log.debug("dgstBits:" + dgstBits);

			// unsigned char:div_bits
			short divBits = buff[pos];
			pos += 1;
			log.debug("divBits:" + divBits);

			// unsigned char:rn
			short rn = buff[pos];
			pos += 1;
			log.debug("rn:" + rn);

			// # unsigned short:number of nodes
			int numOfNodes = (buff[pos] << 8) & 0xff00 | buff[pos + 1] & 0xff;
			pos += 2;
			log.debug("numOfNodes:" + numOfNodes);

			// # string:node-id
			String[] nodeId = new String[numOfNodes];
			for (int i=0; i < numOfNodes; i++) {
				int tmpLen = (buff[pos] << 8) & 0xff00 | buff[pos + 1] & 0xff;
				pos += 2;
				byte[] tmpByte = new byte[tmpLen];
				for (int j=0; j < tmpLen; j++) {
					tmpByte[j] = buff[pos + j];
				}
				nodeId[i] = new String(tmpByte);
				pos += tmpLen;
			}
			log.debug("nodeId:");
			for (int i=0; i < numOfNodes; i++) {
				log.debug(nodeId[i]);
			}
			
			// int32_v_clk / index of nodes
			// map key=vnode val=[0]v_clk, [1..n]node_id
			HashMap<Long, Long> vClk = new HashMap<Long, Long>();
			HashMap<Long, int[]> vNode = new HashMap<Long, int[]>();
			int[] tmpNodes = null;
			for (int i=0; i < Math.pow(2, divBits); i++) {
				long vn = (long)i << (dgstBits - divBits);
				//log.debug("vn:" + vn);
				long tmpClk = (buff[pos] << 24) & 0xff000000L |
							(buff[pos + 1] << 16) & 0xff0000L |
							(buff[pos + 2] << 8) & 0xff00L |
							buff[pos + 3] & 0xffL;
				//log.debug("tmpClk:" + tmpClk);				
				short tmpNumOfNodes = buff[pos + 4];
				//log.debug("tmpNumOfNodes:" + tmpNumOfNodes);
				pos += 5;
				vClk.put(vn, tmpClk);
				tmpNodes = new int[rn];
				for (int j=0; j < tmpNumOfNodes; j++) {
					int tmpIdx = (buff[pos] << 8) & 0xff00 | buff[pos + 1] & 0xff;
					pos += 2;
					tmpNodes[j] = tmpIdx;
					//log.debug("tmpIdx:" + tmpIdx);
				}
				vNode.put(vn, tmpNodes);
			}


			// Store to HashMap
			routingData.setFormatVer(formatVer);
			routingData.setDgstBits(dgstBits);
			routingData.setDivBits(divBits);
			routingData.setRn(rn);
			routingData.setNumOfNodes(numOfNodes);
			routingData.setNodeId(nodeId);
			routingData.setVClk(vClk);
			routingData.setVNode(vNode);

			return routingData;

		} catch (Exception e) {
			//socket.close();
			e.printStackTrace();
			throw new Exception("RoutingDump Exception.");
		}		
	}
	
	public Connection getConnection(String key){
	    // TODO
	    return null;
	}

	public void returnConnection(Connection con){
	    // TODO
	}
	
	public void failCount(){
	    // TODO
	}
}
