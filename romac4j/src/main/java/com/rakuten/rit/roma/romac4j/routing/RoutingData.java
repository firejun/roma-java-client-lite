package com.rakuten.rit.roma.romac4j.routing;

import java.util.HashMap;

public class RoutingData {
	private int formatVer = 0;
	private short dgstBits = 0;
	private short divBits = 0;
	private short rn = 0;
	private int numOfNodes = 0;
	private String[] nodeId = null;
	private HashMap<Long, Long> vClk = null;
	private HashMap<Long, int[]> vNode = null;

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
