/**
 * 
 */
package com.uob.edag.model;

import java.text.MessageFormat;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;

/**
 * @author Muhammad Bilal
 *
 */
public class ProcParam {
	
	private static final MessageFormat INSERT_PROC_PARAM_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROC_PARAM(PROC_ID, PARAM_NM, PARAM_VAL, CRT_DT, CRT_USR_NM, UPD_DT, UPD_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', DEFAULT, ''{3}'', DEFAULT, ''{3}''); ")
	);
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private String procId;
	private String paramNm;
	private String paramVal;
	
	public String getInsertProcParamSql() {
		String result = INSERT_PROC_PARAM_TEMPLATE.format(new Object[] {getProcId(), getParamNm(), getParamVal(), 
					                                                     			UobConstants.SYS_USER_NAME});
		logger.debug("Insert proc param statements: " + result);
		return result;
	}

	public String getProcId() {
		return procId;
	}

	public void setProcId(String procId) {
		this.procId = procId == null ? null : procId.trim();
	}

	public String getParamNm() {
		return paramNm;
	}

	public void setParamNm(String paramNm) {
		this.paramNm = paramNm == null ? null : paramNm.trim();
	}

	public String getParamVal() {
		return paramVal;
	}

	public void setParamVal(String paramVal) {
		this.paramVal = paramVal == null ? null : paramVal.trim();
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("procId", procId)
				                            .append("paramNm", paramNm)
				                            .append("paramVal", paramVal)
				                            .toString();
	}
}
