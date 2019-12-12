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
public class ProcDownstreamAppl {
	
	private static final MessageFormat INSERT_PROC_DOWNSTREAM_APPL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROC_DOWNSTREAM_APPL(PROC_ID, APPL_CD, CRT_DT, CRT_USR_NM, UPD_DT, UPD_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', DEFAULT, ''{2}'', DEFAULT, ''{2}''); ")
	);
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private String procId;
	private String applCd;
	
	public String getInsertProcDownstreamApplSql() {
		String result = INSERT_PROC_DOWNSTREAM_APPL_TEMPLATE.format(new Object[] {getProcId(), getApplCd(), 
																															 							  UobConstants.SYS_USER_NAME});
		logger.debug("Insert proc downstream appl statement: " + result);
		return result;
	}

	public String getProcId() {
		return procId;
	}

	public void setProcId(String procId) {
		this.procId = procId;
	}

	public String getApplCd() {
		return applCd;
	}

	public void setApplCd(String applCd) {
		this.applCd = applCd;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("procId", procId)
				                            .append("applCd", applCd)
				                            .toString();
	}
}
