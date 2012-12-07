package com.rakuten.rit.roma.romac4j.commands;

import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.rakuten.rit.roma.romac4j.utils.StringUtils;

public class BasicCommands {
	protected static Logger log = Logger.getLogger(BasicCommands.class.getName());

	public static byte[] get(String key, Socket socket, Properties props) {
		BufferedInputStream is = null;
		PrintWriter writer = null;
		String[] header = null;
		String str = null;
		byte[] buff = null;
		int iVal = 0;
		
		try {
			// Output stream open
			writer = new PrintWriter(socket.getOutputStream(), true);

			// Execute command
			writer.write("get " + key + "\n");
			writer.flush();

			// Receive header part
			is = new BufferedInputStream(socket.getInputStream());
			str = StringUtils.readOneLine(is,
					Integer.valueOf(props.getProperty("bufferSize")));

			// Analyze header part
			header = str.split(" ");
			if (header.length == 4) {
				iVal = Integer.valueOf(header[3]);

				// Initialize buffer
				buff = new byte[iVal];

				// Read from stream
				is.read(buff, 0, iVal);
			}
		} catch (Exception e) {
			log.error("Get failed.");
		}
		return buff;
	}
}
