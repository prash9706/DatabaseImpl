package com.demodb.dto;

public class ResultSet {
	private int rowAffected;
	private String data;
	
	public ResultSet(){
		this.rowAffected = 0;
		this.data = "";
	}
	
	public ResultSet(int rowAffected){
		this.rowAffected = rowAffected;
		this.data = "";
	}

	public int getRowAffected() {
		return rowAffected;
	}

	public void setRowAffected(int rowAffected) {
		this.rowAffected = rowAffected;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}
	
	
}
