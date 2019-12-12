package com.uob.edag.model;

import org.apache.commons.lang3.StringUtils;

import com.uob.edag.utils.PropertyLoader;

public class CountryAttributes {
	
	public enum FRREmptyBizDateControl {FAIL, SKIP, PREVIOUS, PREVIOUS_THEN_FAIL, NORMAL};

	private String countryCode;
	private String charset = "default";
	private String encoding = "default";
	private FRREmptyBizDateControl frrEmptyBizDateControl = FRREmptyBizDateControl.NORMAL;
	private int frrMapperCount = -1;
	private String referencedFileFolder = null;
	
	public CountryAttributes() {
		// default constructor
	}
	
	public void setReferencedFileFolder(String folder) {
		this.referencedFileFolder = folder;
	}
	
	public String getReferencedFileFolder() {
		return this.referencedFileFolder;
	}
	
	public void setFRRLoaderMapperCount(int count) {
		this.frrMapperCount = count;
	}
	
	public int getFRRLoaderMapperCount() {
		return this.frrMapperCount;
	}
	
	public void setFRRLoaderEmptyBizDateControl(String value) {
		this.frrEmptyBizDateControl = StringUtils.isBlank(value) ? FRREmptyBizDateControl.NORMAL 
				                                                     : FRREmptyBizDateControl.valueOf(value.toUpperCase());
	}
	
	public FRREmptyBizDateControl getFRRLoaderEmptyBizDateControl() {
		return this.frrEmptyBizDateControl;
	}
	
	public CountryAttributes(String countryCode) {
		setCountryCode(countryCode);
	}
	
	public void setCountryCode(String countryCode) {
		this.countryCode = StringUtils.trimToEmpty(countryCode);
	}
	
	public String getCountryCode() {
		return this.countryCode;
	}
	
	public void setCharset(String charset) {
		this.charset = StringUtils.trimToEmpty(charset);
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	public String getCharset() {
		return getCharset(false);
	}
	
	public String getEncoding() {
		return getEncoding(false);
	}
	
	public String getCharset(boolean resolve) {
		return (resolve && "default".equalsIgnoreCase(charset)) ? PropertyLoader.getCharsetName(countryCode) : charset;
	}
	
	public String getEncoding(boolean resolve) {
		return (resolve && "default".equalsIgnoreCase(encoding)) ? PropertyLoader.getEncoding(countryCode) : encoding;
	}
}
