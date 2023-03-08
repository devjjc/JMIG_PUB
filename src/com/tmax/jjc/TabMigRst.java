package com.tmax.jjc;

public class TabMigRst {
	private String verify = null;
	private long elapsed_ms = 0;
	private long row_cnt = 0;
	
	

	public TabMigRst(String verify, long elapsed_ms, long row_cnt ) {
		this.verify = verify;
		this.elapsed_ms = elapsed_ms;
		this.row_cnt = row_cnt;
	}
	
	public String getVerify() {
		return this.verify;
	}
	
	public long getElapsedMs() {
		return this.elapsed_ms;
	}
	
	public long getRowCnt() {
		return this.row_cnt;
	}
	// getters and setters

}
