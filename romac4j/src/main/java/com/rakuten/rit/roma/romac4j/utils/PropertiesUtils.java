package com.rakuten.rit.roma.romac4j.utils;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertiesUtils {
    private static final String CONFIG_FILE = "romaclient.properties";
    private static final String DEFAULT_ROMA_ADDRESS = "localhost_11111";
    private static final String DEFAULT_MAX_ACTIVE = "10";
    private static final String DEFAULT_MAX_IDLE = "10";
    private static final String DEFAULT_TIMEOUT = "1000";
    private static final String DEFAULT_EXP_TIMEOUT = "10000";
    private static final String DEFAULT_BUFFER_SIZE = "1024";
    private static final String DEFAULT_MAX_RETRY = "5";
    private static final String DEFAULT_FAIL_COUNT = "10";
    private static final String DEFAULT_THREAD_SLEEP = "5000";

    public PropertiesUtils() {
    }

    public static Properties getRomaClientProperties() {
        Properties props = new Properties();
        props.setProperty("address_port", DEFAULT_ROMA_ADDRESS);
        props.setProperty("maxActive", DEFAULT_MAX_ACTIVE);
        props.setProperty("maxIdle", DEFAULT_MAX_IDLE);
        props.setProperty("timeout", DEFAULT_TIMEOUT);
        props.setProperty("expTimeout", DEFAULT_EXP_TIMEOUT);
        props.setProperty("bufferSize", DEFAULT_BUFFER_SIZE);
        props.setProperty("maxRetry", DEFAULT_MAX_RETRY);
        props.setProperty("failCount", DEFAULT_FAIL_COUNT);
        props.setProperty("threadSleep", DEFAULT_THREAD_SLEEP);
        try {
            FileInputStream pFile = new FileInputStream(CONFIG_FILE);
            props.load(pFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }
}
