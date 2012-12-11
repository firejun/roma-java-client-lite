package com.rakuten.rit.roma.romac4j.utils;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertiesUtils {
	private Properties props;
	private static final String CONFIG_FILE = "romaclient.properties";
	private static final String DEFAULT_ROMA_ADDRESS = "localhost";
	private static final String DEFAULT_ROMA_PORT = "11211";
	private static final String DEFAULT_NUM_OF_CONNECTION = "10";
	private static final String DEFAULT_MAX_ACTIVE = "10";
	private static final String DEFAULT_MAX_IDLE = "10";
	private static final String DEFAULT_TIMEOUT = "1000";
	private static final String DEFAULT_BUFFER_SIZE = "1024";

	public PropertiesUtils() {
		props = new Properties();
		props.setProperty("address", DEFAULT_ROMA_ADDRESS);
		props.setProperty("port", DEFAULT_ROMA_PORT);
		props.setProperty("numOfConnection", DEFAULT_NUM_OF_CONNECTION);
		props.setProperty("maxActive", DEFAULT_MAX_ACTIVE);
		props.setProperty("maxIdle", DEFAULT_MAX_IDLE);
		props.setProperty("timeout", DEFAULT_TIMEOUT);
		props.setProperty("bufferSize", DEFAULT_BUFFER_SIZE);
		try {
			FileInputStream pFile = new FileInputStream(CONFIG_FILE);
			props.load(pFile);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Properties getRomaClientProperties() {
		return props;
	}

	public void setTimeout(int timeout) {
		props.setProperty("timeout", String.valueOf(timeout));
	}
}
