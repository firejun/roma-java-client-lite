package com.rakuten.rit.roma.romac4j.routing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

public class MerkleTree {
    private HashMap<String, String> tree = new  HashMap<String, String>();
    private int dgstBits, divBits;
    private MessageDigest md;
    
    public MerkleTree(int _dgstBits, int _divBits) throws NoSuchAlgorithmException{
        md = MessageDigest.getInstance("SHA1");
        dgstBits = _dgstBits;
        divBits = _divBits;
        createTreeInstance("0");
    }
    
    public void set(Long vn, String nodes){
        String id = "0" + rjust(Long.toBinaryString(vn >> (dgstBits - divBits)));
        tree.put(id, hexdigest(nodes));
        update(parent(id));
    }
    
    public String get(String id){
        return tree.get(id);
    }
    
    public long toVn(String id){
        return Long.parseLong(id.substring(1,id.length()), 2) << (dgstBits - divBits);
    }
    
    private String rjust(String id){
        String zero = "";
        for(int i = 0;i < divBits - id.length(); i++) zero += "0";
        return zero + id;
    }
    
    private void createTreeInstance(String id){
        tree.put(id, "");
        if(id.length() > divBits) return;
        createTreeInstance(id + "0");
        createTreeInstance(id + "1");
    }
    
    private void update(String id){
        String c0 = tree.get(id + "0");
        String c1 = tree.get(id + "1");
        tree.put(id, hexdigest(c0 + ":" + c1));
        if(id.length() != 1) update(parent(id));
    }
    
    private String parent(String id){
        return id.substring(0, id.length() - 1);
    }
    
    private String hexdigest(String input){
        md.update(input.getBytes());
        byte[] d = md.digest();
        String ret = "";
        for(int i = 0;i < d.length; i ++){
            ret += String.format("%02x", d[i]);
        }
        return ret;
    }
}
