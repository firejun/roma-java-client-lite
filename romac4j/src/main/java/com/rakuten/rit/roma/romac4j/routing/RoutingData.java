package com.rakuten.rit.roma.romac4j.routing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

public class RoutingData {
    protected static Logger log = Logger.getLogger(RoutingData.class.getName());

    private short dgstBits = 0;
    private short divBits = 0;
    private short rn = 0;
    private String[] nodeId = null;
    private HashMap<Long, Long> vClk = null;
    private HashMap<Long, String[]> vNode = null;
    private MerkleTree mtree = null;
    private Random rnd = new Random(System.currentTimeMillis());

    class BinUtil {
        int pos;
        byte[] bin;
        
        BinUtil(byte[] _bin){
            bin = _bin;
            pos = 0;
        }
        
        byte getByte(){
            return bin[pos++];
        }
        
        int getUInt16(){
            return (bin[pos++] << 8) & 0xff00 | bin[pos++] & 0xff;
        }
        
        long getUInt32(){
            return (bin[pos++] << 24) & 0xff000000L |
                    (bin[pos++] << 16) & 0xff0000L |
                    (bin[pos++] << 8) & 0xff00L |
                    bin[pos++] & 0xffL;
        }
        
        String getString(int n){
            String ret =  new String(bin, pos, n);
            pos += n;
            return ret;
        }
    }
    
    private RoutingData() {}
    
    public RoutingData(byte[] _bin) throws ParseException, NoSuchAlgorithmException {
        BinUtil bin = new BinUtil(_bin);
        
        if( !bin.getString(2).equals("RT") ){
            log.debug("This is not RT Data.");
            throw new ParseException("Illegal Format.", 0);
        }

        int formatVer = bin.getUInt16();    // unsigned short:format version
        if(formatVer != 1){
            throw new ParseException("Unsupported version.", formatVer);
        }
        dgstBits = bin.getByte();       // unsigned char:dgst_bits
        divBits = bin.getByte();        // unsigned char:div_bits
        rn = bin.getByte();             // unsigned char:rn
        int numOfNodes = bin.getUInt16();   // unsigned short:number of nodes
        nodeId = new String[numOfNodes];
        for (int i = 0; i < numOfNodes; i++) {
            nodeId[i] = bin.getString(bin.getUInt16()); // string:node-id
        }
        
        // int32_v_clk / index of nodes
        // map key=vnode val=[0]v_clk, [1..n]node_id
        vClk = new HashMap<Long, Long>();
        vNode = new HashMap<Long, String[]>();
        String[] tmpNodes = null;
        for (int i = 0; i < Math.pow(2, divBits); i++) {
            long vn = (long) i << (dgstBits - divBits);
            // log.debug("vn:" + vn);
            vClk.put(vn, bin.getUInt32());
            // log.debug("tmpClk:" + vClk.get(vn));
            short tmpNumOfNodes = bin.getByte();
            // log.debug("tmpNumOfNodes:" + tmpNumOfNodes);
            tmpNodes = new String[tmpNumOfNodes];
            for (int j = 0; j < tmpNumOfNodes; j++) {
                tmpNodes[j] = nodeId[bin.getUInt16()];
            }
            vNode.put(vn, tmpNodes);
        }
        
        initMklTree();
    }

    public String getPrimaryNodeId(String key) throws NoSuchAlgorithmException{
        long vn = getVn(key);
        String[] nodes = vNode.get(vn);
        if(nodes == null || nodes.length == 0) return null;
        return nodes[0];
    }
    
    public long getVn(String key) throws NoSuchAlgorithmException {
        long mask = ((1L << divBits) - 1) << (dgstBits - divBits);
        MessageDigest md = MessageDigest.getInstance("SHA1");
        md.update(key.getBytes());
        byte[] b = md.digest();
        long h = ((long) b[b.length - 7] << 48) & 0xff000000000000L
                | ((long) b[b.length - 6] << 40) & 0xff0000000000L
                | ((long) b[b.length - 5] << 32) & 0xff00000000L
                | ((long) b[b.length - 4] << 24) & 0xff000000L
                | ((long) b[b.length - 3] << 16) & 0xff0000L
                | ((long) b[b.length - 2] << 8) & 0xff00L
                | (long) b[b.length - 1] & 0xffL;
        return h & mask;
    }

    public String getMklHash(String id){
        return mtree.get(id);
    }
    
    private void initMklTree() throws NoSuchAlgorithmException {
        mtree = new MerkleTree(dgstBits, divBits);
        
        for(long vn : vNode.keySet()){
            mtree.set(vn, vnNodesString(vn));
        }
    }
    
    private String vnNodesString(long vn) {
        String[] nodes = vNode.get(vn);
        if(nodes == null || nodes.length == 0) return "[]";
        String ret = "[\"" + nodes[0] + "\"";
        
        for(int i = 1; i < nodes.length; i++) {
            ret += ", \"" + nodes[i] + "\"";
        }
        return ret + "]";
    }
    
    public RoutingData failOver(String rmnode) {
        RoutingData ret = new RoutingData();
        
        ret.dgstBits = dgstBits;
        ret.divBits = divBits;
        ret.rn = rn;
        ret.nodeId = new String[nodeId.length - 1];

        ArrayList<String> nodes = new ArrayList<String>();
        for(int i = 0; i < nodeId.length; i++){
            if( !nodeId[i].equals(rmnode) ) nodes.add(nodeId[i]);
        }
        ret.nodeId = (String[]) nodes.toArray();
        ret.vClk = new HashMap<Long, Long>();
        ret.vNode = new HashMap<Long, String[]>();

        for(long vn : vNode.keySet()) {
            long clk = vClk.get(vn);
            nodes = new ArrayList<String>();
            for(String nid : vNode.get(vn)){
                if( nid.equals(rmnode) ) {
                    clk ++;
                } else {
                    nodes.add(nid);
                }
            }
            ret.vNode.put(vn, (String[]) nodes.toArray());
            ret.vClk.put(vn, clk);
        }
        
        try {
            ret.initMklTree();
        } catch (NoSuchAlgorithmException e) {
            log.error("failOver() : " + e.getMessage());
            // fatal error : stop an application
            throw new RuntimeException("fatal : " + e.getMessage());
        }
        
        return ret;
    }

    public String getRandomNodeId() {
        return nodeId[rnd.nextInt(nodeId.length)];
    }
}
