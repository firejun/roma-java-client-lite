package com.rakuten.rit.roma.romac4j;

import java.io.IOException;

import org.apache.log4j.Logger;

public class MapPluginClient extends ClientObject {
	protected static Logger log = Logger.getLogger(MapPluginClient.class
			.getName());

	public MapPluginClient(ClientObject obj) {
		super(obj);
	}

	public MapPluginClient(String nodeId) {
		super(nodeId);
	}

	public boolean set(String key, String mapKey, byte[] value, int expt)
			throws IOException {
		return sendCmdS("map_set", key,
				"" + mapKey + " 0 " + expt + " " + value.length, value)
				.isStored();
	}

	public boolean set(String key, String mapKey, byte[] value)
			throws IOException {
		return set(key, mapKey, value, 0);
	}

	public boolean setString(String key, String mapKey, String value)
			throws IOException {
		return set(key, mapKey, value.getBytes(), 0);
	}

	public byte[] get(String key, String mapKey) throws IOException {
		return sendCmdV("map_get", key, mapKey).getValue();
	}

	public String getString(String key, String mapKey) throws IOException {
		return sendCmdV("map_get", key, mapKey).getValueString();
	}

	public int delete(String key, String mapKey) throws IOException {
		String ret = sendCmdS("map_delete", key, mapKey).toString();
		if (ret.equals("NOT_DELETED"))
			return 0;
		else if (ret.equals("DELETED"))
			return 1;
		else if (ret.equals("NOT_FOUND"))
			return -2;
		return -1; // error
	}

	public int clear(String key) throws IOException {
		String ret = sendCmdS("map_clear", key).toString();
		if (ret.equals("NOT_CLEARED"))
			return 0;
		else if (ret.equals("CLEARED"))
			return 1;
		else if (ret.equals("NOT_FOUND"))
			return -2;
		return -1; // error
	}

	public int size(String key) throws IOException {
		String ret = sendCmdS("map_size", key).toString();
		if (ret.contains("ERROR"))
			return -1;
		else if (ret.equals("NOT_FOUND"))
			return -2;
		return Integer.parseInt(ret);
	}

	public int isKey(String key, String mapKey) throws IOException {
		String ret = sendCmdS("map_key?", key, mapKey).toString();
		if (ret.equals("false"))
			return 0;
		else if (ret.equals("true"))
			return 1;
		else if (ret.equals("NOT_FOUND"))
			return -2;
		return -1; // error
	}

	public int isValue(String key, byte[] value) throws IOException {
		String ret = sendCmdS("map_value?", key, "" + value.length, value)
				.toString();
		if (ret.equals("false"))
			return 0;
		else if (ret.equals("true"))
			return 1;
		else if (ret.equals("NOT_FOUND"))
			return -2;
		return -1; // error
	}

	public int isEmpty(String key) throws IOException {
		String ret = sendCmdS("map_empty?", key).toString();
		if (ret.equals("false"))
			return 0;
		else if (ret.equals("true"))
			return 1;
		else if (ret.equals("NOT_FOUND"))
			return -2;
		return -1;
	}

	public int keys(String key) throws IOException {
		String ret = sendCmdV("map_keys", key).getValueString();
		if (ret.contains("ERROR"))
			return -1;
		return Integer.parseInt(ret);
	}

	public int values(String key) throws IOException {
		String ret = sendCmdV("map_values", key).getValueString();
		if (ret.contains("ERROR"))
			return -1;
		return Integer.parseInt(ret);
	}

	public String toString(String key) throws IOException {
		return sendCmdV("map_to_s", key).getValueString();
	}
}
