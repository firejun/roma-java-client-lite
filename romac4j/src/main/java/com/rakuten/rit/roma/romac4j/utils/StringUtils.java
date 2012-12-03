package com.rakuten.rit.roma.romac4j.utils;

import java.io.BufferedInputStream;

public class StringUtils {

	public StringUtils() {
	}

	public static String calcVn(String key) {
		// TODO: This is Test Logic.
		return "10.184.17.15_11211";
	}

	public static StringBuffer readOneLine(BufferedInputStream is)
			throws Exception {
		byte[] b = new byte[1];
		StringBuffer sb = new StringBuffer();
		while (true) {
			try {
				is.read(b, 0, 1);
				if (b[0] == 0x0d) {
					is.read(b, 0, 1);
					if (b[0] == 0x0a)
						break;
				}
				sb.append(new String(b));
			} catch (Exception e) {
				e.printStackTrace();
				throw new Exception("Can't convert StringBuffer.");
			}
		}
		return sb;
	}
}
