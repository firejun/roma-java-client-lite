package com.rakuten.rit.roma.romac4j;

import java.io.IOException;

import org.apache.log4j.Logger;

public class AListPluginClient extends ClientObject {
    protected static Logger log = Logger.getLogger(AListPluginClient.class.getName());
    
    public AListPluginClient(ClientObject obj) {
        super(obj);
    }

    public AListPluginClient(String nodeId) {
        super(nodeId);
    }

    public byte[] at(String key, int index) throws IOException {
        return sendCmdV("alist_at", key, "" + index).getValue();
    }

    public String atString(String key, int index) throws IOException {
        return sendCmdV("alist_at", key, "" + index).getValueString();
    }

    public boolean clear(String key) throws IOException {
        return sendCmdS("alist_clear", key).toString().equals("CLEARED");
    }
    
    public boolean delete(String key, byte[] value) throws IOException {
        return sendCmdS("alist_delete",
                key, "" + value.length, value).isDeleted();
    }

    public boolean delete(String key, String value) throws IOException {
        return delete(key, value.getBytes());
    }

    public boolean deleteAt(String key, int index) throws IOException {
        return sendCmdS("alist_delete_at", key, "" + index).isDeleted();
    }

    /**
     * Empty inspection
     * When a return value is bigger than 0, the list is empty or not exist.
     * @param key
     * @return 0:not empty, 1:empty, 2:the key dose't exist, -1:error
     * @throws IOException
     */
    public int isEmpty(String key) throws IOException {
        String ret = sendCmdS("alist_empty?", key).toString();
        if(ret.equals("false")) return 0;
        else if(ret.equals("true")) return 1;
        else if(ret.equals("NOT_FOUND")) return 2;
        return -1;
    }
    
    public byte[] first(String key) throws IOException {
        return sendCmdV("alist_first", key).getValue();
    }

    public String firstString(String key) throws IOException {
        return sendCmdV("alist_first", key).getValueString();
    }

    public byte[][] gets(String key) throws IOException {
        ValueReceiver rcv = sendCmdV("alist_gets", key);
        int len = rcv.size() - 1;
        if(len < 0) len = 0;
        byte[][] ret = new byte[len][];
        for(int i = 0; i < len; i++) {
            ret[i] = rcv.getValue(i + 1);
        }
        return ret;
    }

    public String[] getsString(String key) throws IOException {
        ValueReceiver rcv = sendCmdV("alist_gets", key);
        int len = rcv.size() - 1;
        if(len < 0) len = 0;
        String[] ret = new String[len];
        for(int i = 0; i < len; i++) {
            ret[i] = rcv.getValueString(i + 1);
        }
        return ret;
    }
    
    public class VauleTime {
        private byte[] value;
        private long epochTime;
        
        public VauleTime(byte[] value, long epochTime){
            this.value = value;
            this.epochTime = epochTime;
        }
        
        public byte[] getValue() {
            return value;
        }
        
        public String getValueString() {
            return new String(value);
        }
        
        public long getEpochTime() {
            return epochTime;
        }
    }
    
    public VauleTime[] getsWithTime(String key) throws IOException {
        ValueReceiver rcv = sendCmdV("alist_gets_with_time", key);
        if(rcv.size() == 0) return new VauleTime[0];
        int len = rcv.getValueInt();
        VauleTime[] ret = new VauleTime[len];
        for(int i = 0; i < len; i++) {
            byte[] v = rcv.getValue(i * 2 + 1);
            long t = rcv.getValueLong(i * 2 + 2);
            ret[i] = new VauleTime(v, t);
        }
        return ret;
    }
    
    public int isInclude(String key, byte[] value) throws IOException {
        String ret = sendCmdS("alist_include?",
                key, "" + value.length, value).toString();        
        if(ret.equals("false")) return 0;
        else if(ret.equals("true")) return 1;
        else if(ret.equals("NOT_FOUND")) return -2;
        return -1;  // error
    }
    
    public int isInclude(String key, String value) throws IOException {
        return isInclude(key, value.getBytes());
    }

    public int index(String key, byte[] value) throws IOException {
        String ret = sendCmdS("alist_index", key, "" + value.length, value).toString();
        if(ret.contains("ERROR")) return -1;
        else if(ret.equals("NOT_FOUND")) return -2; // The key does not exist.
        else if(ret.equals("nil")) return -3; // The value does not exist.        
        return Integer.parseInt(ret);
    }
    
    public int index(String key, String value) throws IOException {
        return index(key, value.getBytes());
    }
    
    public boolean insert(String key, int index, byte[] value) throws IOException {
        return sendCmdS("alist_insert", key, "" + index + " " + value.length, value).isStored();
    }

    public boolean insert(String key, int index, String value) throws IOException {
        return insert(key, index, value.getBytes());
    }

    public boolean sizedInsert(String key, int size, byte[] value)
            throws IOException {
        return sendCmdS("alist_sized_insert",
                key, "" + size + " " + value.length, value).isStored();
    }

    public boolean sizedInsert(String key, int size, String value)
            throws IOException {
        return sizedInsert(key, size, value.getBytes());
    }

    public boolean swapAndInsert(String key, byte[] value)
            throws IOException {
        return sendCmdS("alist_swap_and_insert",
                key, "" + value.length, value).isStored();
    }
    
    public boolean swapAndInsert(String key, String value)
            throws IOException {
        return swapAndInsert(key, value.getBytes());
    }
    
    public boolean swapAndSizedInsert(String key, int size, byte[] value)
            throws IOException {
        return sendCmdS("alist_swap_and_sized_insert",
                key, "" + size + " " + value.length, value).isStored();
    }
  
    public boolean swapAndSizedInsert(String key, int size, String value)
            throws IOException {
        return swapAndSizedInsert(key, size, value.getBytes());
    }
    
    public boolean hourExpiredSwapAndInsert(String key, int hexpt, byte[] value)
            throws IOException {
        return sendCmdS("alist_expired_swap_and_insert",
                key, "h" + hexpt + " " + value.length, value).isStored();
    }

    public boolean hourExpiredSwapAndInsert(String key, int hexpt, String value)
            throws IOException {
        return hourExpiredSwapAndInsert(key, hexpt, value.getBytes());
    }

    public boolean dayExpiredSwapAndInsert(String key, int dexpt, byte[] value)
            throws IOException {
        return sendCmdS("alist_expired_swap_and_insert",
                key, "d" + dexpt + " " + value.length, value).isStored();
    }

    public boolean dayExpiredSwapAndInsert(String key, int dexpt, String value)
            throws IOException {
        return dayExpiredSwapAndInsert(key, dexpt, value.getBytes());
    }
    
    public boolean expiredSwapAndInsert(String key, int expt, byte[] value)
            throws IOException {
        return sendCmdS("alist_expired_swap_and_insert",
                key, "" + expt + " " + value.length, value).isStored();
    }

    public boolean expiredSwapAndInsert(String key, int expt, String value)
            throws IOException {
        return expiredSwapAndInsert(key, expt, value.getBytes());
    }
    
    public boolean hourExpiredSwapAndSizedInsert(
            String key, int hexpt, int size, byte[] value) throws IOException {
        return sendCmdS("alist_expired_swap_and_sized_insert",
                key, "h" + hexpt + " " + size + " " + value.length, value).isStored();
    }

    public boolean hourExpiredSwapAndSizedInsert(
            String key, int hexpt, int size, String value) throws IOException {
        return hourExpiredSwapAndSizedInsert(key, hexpt, size, value.getBytes());
    }

    public boolean dayExpiredSwapAndSizedInsert(
            String key, int dexpt, int size, byte[] value) throws IOException {
        return sendCmdS("alist_expired_swap_and_sized_insert",
                key, "d" + dexpt + " " + size + " " + value.length, value).isStored();
    }

    public boolean dayExpiredSwapAndSizedInsert(
            String key, int dexpt, int size, String value) throws IOException {
        return dayExpiredSwapAndSizedInsert(key, dexpt, size, value.getBytes());
    }
    
    public boolean expiredSwapAndSizedInsert(
            String key, int expt, int size, byte[] value) throws IOException {
        return sendCmdS("alist_expired_swap_and_sized_insert",
                key, "" + expt + " " + size + " " + value.length, value).isStored();
    }

    public boolean expiredSwapAndSizedInsert(
            String key, int expt, int size, String value) throws IOException {
        return expiredSwapAndSizedInsert(key, expt, size, value.getBytes());
    }
    
    public String[] joinWithTime(String key, String sep) throws IOException {
        String[] ret = new String[2];
        ValueReceiver rcv = sendCmdV("alist_join_with_time", key);
        if(rcv.size() != 3) return null;
        ret[0] = rcv.getValueString(1);
        ret[1] = rcv.getValueString(2);
        return ret;
    }

    public String join(String key) throws IOException {
        return join(key, ",");
    }

    public String join(String key, String sep) throws IOException {
        return sendCmdV("alist_join", key, "" + sep.length(), sep.getBytes()).getValueString(1);
    }
    
    public byte[] last(String key) throws IOException {
        return sendCmdV("alist_last", key).getValue();
    }

    public String lastString(String key) throws IOException {
        return sendCmdV("alist_last", key).getValueString();
    }
   
    /**
     * Returns the number of elements in the list.
     * If the key dose't exist, returns -2.
     * @param key
     * @return the number of elements in the list, -1:error, -2:the key dose't exist  
     * @throws IOException
     */
    public int length(String key) throws IOException {
        String ret = sendCmdS("alist_length", key).toString();
        if(ret.contains("ERROR")) return -1;
        else if(ret.equals("NOT_FOUND")) return -2; // The key does not exist.
        return Integer.parseInt(ret);
    }
    
    public byte[] pop(String key) throws IOException {
        return sendCmdV("alist_pop", key).getValue();
    }
    
    public String popString(String key) throws IOException {
        return sendCmdV("alist_pop", key).getValueString();
    }

    public boolean push(String key, byte[] value) throws IOException {
        return sendCmdS("alist_push", key, "" + value.length, value).isStored();
    }

    public boolean push(String key, String value) throws IOException {
        return push(key, value.getBytes());
    }
    
    public boolean sizedPush(String key, int size, byte[] value)
            throws IOException {
        return sendCmdS("alist_sized_push",
                key, "" + size + " " + value.length, value).isStored();
    }

    public boolean sizedPush(String key, int size, String value)
            throws IOException {
        return sizedPush(key, size, value.getBytes());
    }

    public boolean swapAndPush(String key, byte[] value) throws IOException {
        return sendCmdS("alist_swap_and_push",
                key, "" + value.length, value).isStored();
    }
    
    public boolean swapAndPush(String key, String value) throws IOException {
        return swapAndPush(key, value.getBytes());
    }
    
    public boolean swapAndSizedPush(String key, int size, byte[] value)
            throws IOException {
        return sendCmdS("alist_swap_and_sized_push",
                key, "" + size + " " + value.length, value).isStored();
    }
    
    public boolean swapAndSizedPush(String key, int size, String value)
            throws IOException {
        return swapAndSizedPush(key, size, value.getBytes());
    }
    
    public boolean expiredSwapAndPush(String key, int expt, byte[] value)
            throws IOException {
        return sendCmdS("alist_expired_swap_and_push",
                key, "" + expt + " " + value.length, value).isStored();
    }
    
    public boolean expiredSwapAndPush(String key, int expt, String value)
            throws IOException {
        return expiredSwapAndPush(key, expt, value.getBytes());
    }
    
    public boolean expiredSwapAndSizedPush(
            String key, int expt, int size, byte[] value) throws IOException {
        return sendCmdS("alist_expired_swap_and_sized_push",
                key, "" + expt + " " + size + " " + value.length, value).isStored();
    }
   
    public boolean expiredSwapAndSizedPush(
            String key, int expt, int size, String value) throws IOException {
        return expiredSwapAndSizedPush(key, expt, size, value.getBytes());
    }

    public int updateAt(String key, int index, byte[] value)  throws IOException {
        String ret = sendCmdS("alist_update_at",
                key, "" + index + " " + value.length, value).toString();
        if(ret.equals("STORED")) return 1;
        else if(ret.equals("NOT_STORED")) return 0;
        else if(ret.equals("NOT_FOUND")) return -2;
        return -1;
    }

    public int updateAt(String key, int index, String value)  throws IOException {
        return updateAt(key, index, value.getBytes());
    }
    
    public byte[] shift(String key) throws IOException {
        return sendCmdV("alist_shift", key).getValue();
    }

    public String shiftString(String key) throws IOException {
        return sendCmdV("alist_shift", key).getValueString();
    }

    public String toS(String key) throws IOException {
        return sendCmdV("alist_to_s", key).getValueString(1);
    }
}
