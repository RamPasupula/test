package com.uob.edag.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class BizDateRowCount implements Comparable<BizDateRowCount> {
	
	private String bizDate;
	private int rowCount;
	
	public BizDateRowCount() {
		// default constructor
	}
		
	public boolean equals(Object o) {
		try {
			BizDateRowCount other = (BizDateRowCount) o;
			return this.bizDate != null && this.bizDate.equals(other.bizDate);
		} catch (ClassCastException e) {
			return false;
		}
	}
	
	public int hashCode() {
		return this.bizDate == null ? 0 : this.bizDate.hashCode();
	}
	
	public String getBizDate() {
		return bizDate;
	}
	
	public void setBizDate(String bizDate) {
		this.bizDate = bizDate;
	}
	
	public int getRowCount() {
		return this.rowCount;
	}
	
	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	@Override
	public int compareTo(BizDateRowCount o) {
		// descending order
		return o == null || o.bizDate == null ? -1 : o.bizDate.compareTo(this.bizDate);
	}
	
	public String toString() {
		return new ToStringBuilder(this).append("Business date", this.bizDate)
				                            .append("Row count", this.rowCount)
				                            .toString();
	}
}
