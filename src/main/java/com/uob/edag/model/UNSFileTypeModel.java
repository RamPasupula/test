package com.uob.edag.model;

public class UNSFileTypeModel {

	private String fileName;
	private String fileType;
	private String documentTypeCode;
	private String countryCode;
	
	public String getFileName() {
		if (fileName == null) {
			return "NULL";
		}else {
			return fileName.trim();
		}
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getFileType() {
		if (fileType == null) {
			return "NULL";
		}else {
			return fileType.trim();
		}
	}
	public void setFileType(String fileType) {
		this.fileType = fileType;
	}
	public String getCountryCode() {
		if (countryCode == null) {
			return "NULL";
		}else {
			return countryCode.trim();
		}
	}
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	public String getDocumentTypeCode() {
		if (documentTypeCode == null) {
			return "NULL";
		}else {
			return documentTypeCode.trim();
		}
		
	}
	public void setDocumentTypeCode(String documentTypeCode) {
		this.documentTypeCode = documentTypeCode;
	}
	
	
}
