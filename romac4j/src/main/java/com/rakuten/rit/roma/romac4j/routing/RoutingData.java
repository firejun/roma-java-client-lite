package com.rakuten.rit.roma.romac4j.routing;

import java.util.HashMap;

import org.apache.log4j.Logger;

public class RoutingData {
    protected static Logger log = Logger.getLogger(RoutingData.class.getName());

    private int formatVer = 0;
    private short dgstBits = 0;
    private short divBits = 0;
    private short rn = 0;
    private int numOfNodes = 0;
    private String[] nodeId = null;
    private HashMap<Long, Long> vClk = null;
    private HashMap<Long, int[]> vNode = null;

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
    
    public RoutingData(byte[] _bin) throws Exception{
        BinUtil bin = new BinUtil(_bin);
        
        if( !bin.getString(2).equals("RT") ){
            log.debug("This is not RT Data.");
            throw new Exception("Illegal Format.");
        }

        formatVer = bin.getUInt16();    // unsigned short:format version
        dgstBits = bin.getByte();       // unsigned char:dgst_bits
        divBits = bin.getByte();        // unsigned char:div_bits
        rn = bin.getByte();             // unsigned char:rn
        numOfNodes = bin.getUInt16();   // unsigned short:number of nodes
        nodeId = new String[numOfNodes];
        for (int i = 0; i < numOfNodes; i++) {
            nodeId[i] = bin.getString(bin.getUInt16()); // string:node-id
        }
        
        // int32_v_clk / index of nodes
        // map key=vnode val=[0]v_clk, [1..n]node_id
        vClk = new HashMap<Long, Long>();
        vNode = new HashMap<Long, int[]>();
        int[] tmpNodes = null;
        for (int i = 0; i < Math.pow(2, divBits); i++) {
            long vn = (long) i << (dgstBits - divBits);
            // log.debug("vn:" + vn);
            vClk.put(vn, bin.getUInt32());
            // log.debug("tmpClk:" + vClk.get(vn));
            short tmpNumOfNodes = bin.getByte();
            // log.debug("tmpNumOfNodes:" + tmpNumOfNodes);
            tmpNodes = new int[tmpNumOfNodes];
            for (int j = 0; j < tmpNumOfNodes; j++) {
                tmpNodes[j] = bin.getUInt16();
                // log.debug("tmpIdx:" + tmpIdx);
            }
            vNode.put(vn, tmpNodes);
        }
    }

    // public RoutingData failover() {
    // return null;
    // }

    // private RoutingData() {
    public RoutingData() {
    }

    public int getFormatVer() {
        return formatVer;
    }

    public void setFormatVer(int formatVer) {
        this.formatVer = formatVer;
    }

    public short getDgstBits() {
        return dgstBits;
    }

    public void setDgstBits(short dgstBits) {
        this.dgstBits = dgstBits;
    }

    public short getDivBits() {
        return divBits;
    }

    public void setDivBits(short divBits) {
        this.divBits = divBits;
    }

    public short getRn() {
        return rn;
    }

    public void setRn(short rn) {
        this.rn = rn;
    }

    public int getNumOfNodes() {
        return numOfNodes;
    }

    public void setNumOfNodes(int numOfNodes) {
        this.numOfNodes = numOfNodes;
    }

    public String[] getNodeId() {
        return nodeId;
    }

    public void setNodeId(String[] nodeId) {
        this.nodeId = nodeId;
    }

    public HashMap<Long, Long> getVClk() {
        return vClk;
    }

    public void setVClk(HashMap<Long, Long> vClk) {
        this.vClk = vClk;
    }

    public HashMap<Long, int[]> getVNode() {
        return vNode;
    }

    public void setVNode(HashMap<Long, int[]> vNode) {
        this.vNode = vNode;
    }
}
