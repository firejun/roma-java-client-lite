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

	// map_set <key> <mapkey> <flags> <expt> <bytes> [forward]\r\n
	// <data block>\r\n
	//
	// (STORED|NOT_STORED|SERVER_ERROR <error message>)\r\n
	private boolean mapSet(String cmd, String key, String mapKey, byte[] value,
			int expt) throws IOException {
		return sendCmdS(cmd, key,
				"" + mapKey + " 0 " + expt + " " + value.length, value)
				.isStored();
	}

	public boolean mapSet(String key, String mapKey, byte[] value)
			throws IOException {
		return mapSet("map_set", key, mapKey, value, 0);
	}

	public boolean mapSetString(String key, String mapKey, String value)
			throws IOException {
		return mapSet("map_set", key, mapKey, value.getBytes(), 0);
	}

	// map_get <key> <mapkey> [forward]\r\n
	//
	// (
	// [VALUE <key> 0 <value length>\r\n
	// <value>\r\n]
	// END\r\n
	// |SERVER_ERROR <error message>\r\n)
	public byte[] mapGet(String key, String mapKey) throws IOException {
		return sendCmdV("map_get", key, mapKey).getValue();
	}

	public String mapGetString(String key, String mapKey) throws IOException {
		return sendCmdV("map_get", key, mapKey).getValueString();
	}

	// map_delete <key> <mapkey> [forward]\r\n
	//
	// (DELETED|NOT_DELETED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
	public boolean mapDelete(String key, String mapKey) throws IOException {
		return sendCmdS("map_delete", key, mapKey).isDeleted();
	}

	// map_clear <key> [forward]\r\n
	//
	// (CLEARED|NOT_CLEARED|NOT_FOUND|SERVER_ERROR <error message>)\r\n
	public boolean mapClear(String key) throws IOException {
		return sendCmdS("map_clear", key).isCleared();
	}

	// map_size <key> [forward]\r\n
	//
	// (<length>|NOT_FOUND|SERVER_ERROR <error message>)\r\n
	public int mapSize(String key) throws IOException {
		String ret = sendCmdS("map_size", key).toString();
		if(ret.contains("ERROR")) return -1;
		else if(ret.equals("NOT_FOUND")) return -2;
		return Integer.parseInt(ret);
	}

    // map_key? <key> <mapkey> [forward]\r\n
    //
    // (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
    public int isMapKey(String key, String mapKey) throws IOException {
    	String ret = sendCmdS("map_key?", key, mapKey).toString();
        if(ret.equals("false")) return 0;
        else if(ret.equals("true")) return 1;
        else if(ret.equals("NOT_FOUND")) return -2;
        return -1;  // error
    }

    // map_value? <key> <bytes> [forward]\r\n
    // <data block>\r\n
    //
    // (true|false|NOT_FOUND|SERVER_ERROR <error message>)\r\n
    public int mapValue(String key, byte[] value) throws IOException {
    	String ret = sendCmdS("map_value?", key, "" + value.length, value).toString();
        if(ret.equals("false")) return 0;
        else if(ret.equals("true")) return 1;
        else if(ret.equals("NOT_FOUND")) return -2;
        return -1;  // error
    }
}
