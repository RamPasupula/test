package com.uob.edag.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.uob.edag.exception.EDAGValidationException;

public class TDUnsupportedCharacterReplacement {
	
	private String processID;
	private String countryCode;
	private String fieldName;
	private char replacementChar;
	
	public static Map<String, Character> toMap(List<TDUnsupportedCharacterReplacement> source) throws EDAGValidationException {
		Map<String, Character> result = new HashMap<String, Character>();
		
		String procId = null;
		String ctryCd = null;
		
		for (TDUnsupportedCharacterReplacement obj : source) {
			if (procId == null) {
				procId = obj.getProcessID();
			} else if (!procId.equals(obj.getProcessID())) {
				throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, obj.getProcessID(), "Process ID of all elements in the list must be " + procId);
			}
			
			if (ctryCd == null) {
				ctryCd = obj.getCountryCode();
			} else if (!ctryCd.equals(obj.getCountryCode())) {
				throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, obj.getCountryCode(), "Country Code of all elements in the list must be " + ctryCd);
			}
			
			if (result.containsKey(obj.getFieldName().toLowerCase())) {
				throw new EDAGValidationException(EDAGValidationException.DUPLICATED_VALUE, obj.getFieldName(), "Field names must be unique");
			}
			
			result.put(obj.getFieldName().toLowerCase(), obj.getReplacementChar());
		}
		
		return result;
	}
	
	public static void main(String[] args) {
		
	}
	
	public String getProcessID() {
		return this.processID;
	}
	
	public void setProcessID(String procId) {
		this.processID = StringUtils.trimToEmpty(procId);
	}
	
	public String getCountryCode() {
		return this.countryCode;
	}
	
	public void setCountryCode(String ctryCd) {
		this.countryCode = StringUtils.trimToEmpty(ctryCd);
	}
	
	public String getFieldName() {
		return this.fieldName;
	}
	
	public void setFieldName(String fldNm) {
		this.fieldName = StringUtils.trimToEmpty(fldNm);
	}
	
	public char getReplacementChar() {
		return this.replacementChar;
	}
	
	public void setReplacementChar(char chr) {
		this.replacementChar = chr;
	}
}
