package com.rakuten.rit.roma.romac4j.utils;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertiesUtils {
	public PropertiesUtils() {
	}

	/**
	 * 
	 * @return Properties
	 */
	public static Properties preparateProperties() {
		Properties props = new Properties();
		props.setProperty("address", Config.DEFAULT_ROMA_ADDRESS);
		props.setProperty("port", Config.DEFAULT_ROMA_PORT);
		props.setProperty("numOfConnection", Config.DEFAULT_NUM_OF_CONNECTION);
		props.setProperty("maxActive", Config.DEFAULT_MAX_ACTIVE);
		props.setProperty("maxIdle", Config.DEFAULT_MAX_IDLE);
		props.setProperty("timeout", Config.DEFAULT_TIMEOUT);
		try {
			FileInputStream pFile = new FileInputStream(Config.CONFIG_FILE);
			props.load(pFile);
		}catch (Exception e) {
			e.printStackTrace();
		}
		return props;
	}
}
