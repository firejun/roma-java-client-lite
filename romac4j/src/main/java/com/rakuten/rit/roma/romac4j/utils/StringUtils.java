package com.rakuten.rit.roma.romac4j.utils;

import java.io.InputStream;

public class StringUtils {

	public StringUtils() {
	}

	public static String calcVn(String key) {
		// TODO: This is Test Logic.
		return "10.184.17.15_11211";
	}

	public static String readOneLine(InputStream is, int bufferSize)
			throws Exception {
		byte[] b = new byte[1];
		byte[] buff = new byte[bufferSize];
		int i=0;
		while (true) {
			try {
				if (i > bufferSize) {
					throw new ArrayIndexOutOfBoundsException("Too much size.");
				}
				is.read(b, 0, 1);
				if (b[0] == 0x0d) {
					is.read(b, 0, 1);
					if (b[0] == 0x0a)
						break;
				}
				buff[i] = b[0];
				i++;
			} catch (Exception e) {
				throw new Exception("Can't convert header.");
			}
		}
		return new String(buff, 0, i);
	}
}
