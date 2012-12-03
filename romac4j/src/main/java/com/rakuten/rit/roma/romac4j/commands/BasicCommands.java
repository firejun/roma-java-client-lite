package com.rakuten.rit.roma.romac4j.commands;

import java.io.BufferedInputStream;
import java.io.PrintWriter;
import java.net.Socket;

import com.rakuten.rit.roma.romac4j.utils.StringUtils;

public class BasicCommands {
	public static byte[] get(String key, Socket socket) {
		BufferedInputStream is = null;
		PrintWriter writer = null;
		String[] header = null;
		StringBuffer sb = new StringBuffer();
		byte[] buff = null;
		byte[] b = new byte[1];
		int iVal = 0;
		
		try {
			// Output stream open
			writer = new PrintWriter(socket.getOutputStream(), true);

			// Execute command
			writer.write("get " + key + "\n");
			writer.flush();

			// Receive header part
			is = new BufferedInputStream(socket.getInputStream());
			sb = StringUtils.readOneLine(is);

			// Analyze header part
			header = sb.toString().split(" ");
			if (header.length == 4) {
				iVal = Integer.valueOf(header[3]);

				// Initialize buffer
				buff = new byte[iVal];

				// Read from stream
				for (int i = 0; i < iVal; i++) {
					is.read(b, 0, 1);
					buff[i] = b[0];
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return buff;
	}
}
