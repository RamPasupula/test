package com.uob.edag.model;

public class SolrCollectionModel {

	private String countryCode;
	private String fileType;
	private String documentTypeCode;
	private String collectionName;
	private String regularExp;
	private String remarks;
	
	
	public String getFileType() {
		return fileType;
	}
	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
	
	public String getDocumentTypeCode() {
		return documentTypeCode.trim();
	}
	public void setDocumentTypeCode(String documentTypeCode) {
		this.documentTypeCode = documentTypeCode;
	}
	public String getCollectionName() {
		return collectionName.trim();
	}
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	public String getRegularExp() {
		return regularExp.trim();
	}
	public void setRegularExp(String regularExp) {
		this.regularExp = regularExp;
	}
	public String getRemarks() {
		return remarks.trim();
	}
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	public String getCountryCode() {
		return countryCode.trim();
	}
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	
}
