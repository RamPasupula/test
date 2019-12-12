package com.uob.edag.model;

public class AttMasterFileModel {
	
	private String processId;
	private String hiveQL;
	private String engineName;
	private String extendAttr1;
	private String extendAttr2;
	private String extendAttr3;
	private String extendAttr4;
	private String extendAttr5;
	
	public String getEngineName() {
		return engineName;
	}
	public void setEngineName(String engineName) {
		this.engineName = engineName;
	}
	public String getProcessId() {
		return processId.trim();
	}
	public void setProcessId(String processId) {
		this.processId = processId;
	}
	public String getHiveQL() {
		return hiveQL.trim();
	}
	public void setHiveQL(String hiveQL) {
		this.hiveQL = hiveQL;
	}
	public String getExtendAttr1() {
		return extendAttr1;
	}
	public void setExtendAttr1(String extendAttr1) {
		this.extendAttr1 = extendAttr1;
	}
	public String getExtendAttr2() {
		return extendAttr2;
	}
	public void setExtendAttr2(String extendAttr2) {
		this.extendAttr2 = extendAttr2;
	}
	public String getExtendAttr3() {
		return extendAttr3;
	}
	public void setExtendAttr3(String extendAttr3) {
		this.extendAttr3 = extendAttr3;
	}
	public String getExtendAttr4() {
		return extendAttr4;
	}
	public void setExtendAttr4(String extendAttr4) {
		this.extendAttr4 = extendAttr4;
	}
	public String getExtendAttr5() {
		return extendAttr5;
	}
	public void setExtendAttr5(String extendAttr5) {
		this.extendAttr5 = extendAttr5;
	}
	
	

}
