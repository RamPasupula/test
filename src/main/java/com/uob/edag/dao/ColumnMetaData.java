package com.uob.edag.dao;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ColumnMetaData {

	private String name;
	private String label;
	private int displaySize;
	private int type;
	private int precision;
	private int scale;
	private String typeName;
	private String className;
	
	public String toString() {
		return new ToStringBuilder(this).append("name", name)
				                            .append("label", label)
				                            .append("displaySize", displaySize)
				                            .append("type", type)
				                            .append("precision", precision)
				                            .append("scale", scale)
				                            .append("typeName", typeName)
				                            .append("className", className).toString();
	}
	
	ColumnMetaData(String name) {
	  this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public void setLabel(String columnLabel) {
		this.label = columnLabel;
	}
	
	public String getLabel() {
		return this.label;
	}

	public void setDisplaySize(int columnDisplaySize) {
		this.displaySize = columnDisplaySize;
	}
	
	public int getDisplaySize() {
		return this.displaySize;
	}

	public void setType(int columnType) {
		this.type = columnType;
	}
	
	public int getType() {
		return this.type;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}
	
  public int getPrecision() {
  	return this.precision;
  }

	public void setScale(int scale) {
		this.scale = scale;
	}
	
	public int getScale() {
		return this.scale;
	}

	public void setTypeName(String columnTypeName) {
		this.typeName = columnTypeName;
	}

	public String getTypeName() {
		return this.typeName;
	}

	public void setClassName(String columnClassName) {
		this.className = columnClassName;
	}
	
  public String getClassName() {
  	return this.className;
  }
}
