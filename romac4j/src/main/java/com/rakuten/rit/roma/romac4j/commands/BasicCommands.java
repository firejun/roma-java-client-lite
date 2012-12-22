package com.rakuten.rit.roma.romac4j.commands;

import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.utils.StringUtils;

@Deprecated
public class BasicCommands {
	protected static Logger log = Logger.getLogger(BasicCommands.class.getName());

	public byte[] get(String key, Socket socket, Properties props) {
		byte[] result = null;
		
		try {
			// Output stream open
			PrintWriter writer = new PrintWriter(socket.getOutputStream(),
					true);

			// Execute command
			writer.write("get " + key + new String(new byte[] { 0x0a, 0x0d }));
			writer.flush();

			// Receive header part
			BufferedInputStream is = new BufferedInputStream(socket
					.getInputStream());
			String str = StringUtils.readOneLine(is, Integer.valueOf(props
					.getProperty("bufferSize")));

			// Analyze header part
			String[] header = str.split(" ");
			if (header.length == 4) {
				int iVal = Integer.valueOf(header[3]);

				// Initialize buffer
				byte[] b = new byte[Integer.valueOf(props.getProperty("bufferSize"))];
				byte[] buff = new byte[iVal + 7];
				result = new byte[iVal];

				// Read from stream
				int receiveCount = 0;
				int count = 0;
				while (receiveCount < iVal + 7) {
					count = is.read(b, 0, Integer.valueOf(props.getProperty("bufferSize")));
					System.arraycopy(b, 0, buff, receiveCount, count);
					receiveCount += count;
				}
				System.arraycopy(buff, 0, result, 0, iVal);
			}
		} catch (Exception e) {
			log.error("Get failed.");
		}
		return result;
	}
}
