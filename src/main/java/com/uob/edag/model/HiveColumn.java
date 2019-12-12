package com.uob.edag.model;

public class HiveColumn {

	private String col_name;
	private String data_type;
	private String comment;
	
	public String getCol_name() {
		return col_name;
	}
	
	public void setCol_name(String name) {
		this.col_name = name;
	}
	
	public String getData_type() {
		return this.data_type;
	}
	
	public void setData_type(String type) {
		this.data_type = type;
	}
	
	public String getComment() {
		return this.comment;
	}
	
	public void setComment(String comment) {
		this.comment = comment;
	}
}
