/**
 * 
 */
package com.uob.edag.model;

import java.text.MessageFormat;
import java.util.Date;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;

/**
 * @author Muhammad Bilal
 *
 */
public class SourceTableDetail {
	
	private static final MessageFormat INSERT_SOURCE_TABLE_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_SOURCE_TABLE_DETAIL(PROC_ID, SRC_DB_CONNECTION_NM, SRC_SCHEMA_NM, SRC_TBL_NM, ") +
      UobUtils.ltrim("                                     CRT_DT, CRT_USR_NM, UPD_DT, UPD_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', DEFAULT, ''{4}'', DEFAULT, ''{4}''); ")
	);
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private String procId;
	private String srcDbConnectionNm;
	private String srcSchemaNm;
	private String srcTblNm;
	private Date createdDatetime;
	private String createdBy;
	private Date lastUpdatedDatetime;
	private String lastUpdatedBy;
	
	public Date getCreatedDatetime() {
		return this.createdDatetime;
	}
	
	public void setCreatedDatetime(Date dt) {
		this.createdDatetime = dt;
	}
	
	public String getCreatedBy() {
		return this.createdBy;
	}
	
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	
	public Date getLastUpdatedDatetime() {
		return this.lastUpdatedDatetime;
	}
	
	public void setLastUpdatedDatetime(Date when) {
		this.lastUpdatedDatetime = when;
	}
	
	public String getLastUpdatedBy() {
		return this.lastUpdatedBy;
	}
	
	public void setLastUpdatedBy(String by) {
		this.lastUpdatedBy = by;
	}
	
	public String getInsertSourceTableSql() {
		String result = INSERT_SOURCE_TABLE_TEMPLATE.format(new Object[] {getProcId(), getSrcDbConnectionNm(),
																														 				  getSrcSchemaNm(), getSrcTblNm(),
																														 				  UobConstants.SYS_USER_NAME});
		logger.debug("Insert source table statement: " + result);
		return result;
	}

	public String getProcId() {
		return procId;
	}

	public void setProcId(String procId) {
		this.procId = procId == null ? null : procId.trim();
	}

	public String getSrcDbConnectionNm() {
		return srcDbConnectionNm;
	}

	public void setSrcDbConnectionNm(String srcDbConnectionNm) {
		this.srcDbConnectionNm = srcDbConnectionNm == null ? null : srcDbConnectionNm.trim();
	}

	public String getSrcSchemaNm() {
		return srcSchemaNm;
	}

	public void setSrcSchemaNm(String srcSchemaNm) {
		this.srcSchemaNm = srcSchemaNm == null ? null : srcSchemaNm.trim();
	}

	public String getSrcTblNm() {
		return srcTblNm;
	}

	public void setSrcTblNm(String srcTblNm) {
		this.srcTblNm = srcTblNm == null ? null : srcTblNm.trim();
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("procId", procId)
				                            .append("srcDbConnectionNm", srcDbConnectionNm)
				                            .append("srcSchemaNm", srcSchemaNm)
				                            .append("srcTblNm", srcTblNm)
				                            .toString();
	}
}
