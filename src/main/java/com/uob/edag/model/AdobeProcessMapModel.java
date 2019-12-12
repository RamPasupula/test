package com.uob.edag.model;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;

public class AdobeProcessMapModel {
	private List<String> procIdList = new ArrayList<String>();
	private String masterProcId = new String();
	protected Logger logger = Logger.getLogger(getClass());
	private String procDesc;
	private String procFreqCd;
	
	private static final MessageFormat REMOVE_PROC_SUB_PROC_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_ADB_PROC_SUB_PROC_MAP WHERE MASTER_PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat INSERT_PROC_SUB_PROC_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_ADB_PROC_SUB_PROC_MAP (MASTER_PROC_ID, PROC_ID) ") +
		      UobUtils.ltrim("VALUES(''{0}'', ''{1}''); ")
	);
	
	private static final MessageFormat REMOVE_PROC_MASTER_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_PROCESS_MASTER WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat INSERT_PROC_MASTER_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROCESS_MASTER (PROC_ID, PROC_NM, PROC_TYPE_CD, PROC_GRP_ID, PROC_DESC, PROC_FREQ_CD, SRC_SYS_CD, DEPLOY_NODE_NM, PROC_CRITICALITY_CD, IS_ACT_FLG, CRT_DT, CRT_USR_NM) ") +
		      UobUtils.ltrim("VALUES(''{0}'', ''{1}'',''{2}'', ''{3}'',''{4}'', ''{5}'',''{6}'', ''{7}'',''{8}'', ''{9}'', DEFAULT, ''{10}''); ")
	);
			
	public String getRemoveAdobeProcSubProcSql() {
		String result = REMOVE_PROC_SUB_PROC_TEMPLATE.format(new Object[] {getMasterProcId()});
		logger.debug("Remove source table detail statement: " + result);
		return result;
	}
	
	public String getInsertAdobeProcSubProcSql(String procIdParam) {
		String result = INSERT_PROC_SUB_PROC_TEMPLATE.format(new Object[] {getMasterProcId(),procIdParam}
															);
		logger.debug("Insert to process master SQL: " + result);
		return result;
	}
	
	public String getInsertAdobeProcMasterSql() {
		String result = INSERT_PROC_MASTER_TEMPLATE.format(new Object[] {getMasterProcId(),getMasterProcId(),
																		 getProcTypeCd(), getProcGrpId(),
																		 getProcDesc(), getProcFreqCd(),
																		 getSrcSysCd(), getDeployNodeNm(),
																		 getProcCriticalityCd(), getIsActFlg(),
																		 UobConstants.SYS_USER_NAME});
		logger.debug("Insert to process master SQL: " + result);
		return result;
	}
	
	public String getRemoveAdobeProcMasterSql() {
		String result = REMOVE_PROC_MASTER_TEMPLATE.format(new Object[] {getMasterProcId()});
		logger.debug("Remove Adobe entry from process master SQL: " + result);
		return result;
	}
	
	public void setMasterProcId(String mstProcId) {
		this.masterProcId = mstProcId;
	}
	
	public String getMasterProcId() {
		return masterProcId;
	}
	
	public List<String> getProcId() {
		return procIdList;
	}
	
	public void setProcId(String procId) {
		if(procId != null)
			this.procIdList.add(procId.trim());
		else
			this.procIdList=null;
	}
	
	public String getProcTypeCd() {
		return UobConstants.ADOBE_SITE_CATALYST_PROC_TYP_CD;
	}
	
	public String getProcGrpId() {
		return UobConstants.ADOBE_SITE_CATALYST_PROC_GRP_ID;
	}
	
	public void setProcDesc(String procDesc) {
		this.procDesc = procDesc;
	}

	public String getProcDesc() {
		return procDesc;
	}
	
	public void setProcFreqCd(String procFreqCd) {
		this.procFreqCd = procFreqCd;
	}

	public String getProcFreqCd() {
		return procFreqCd;
	}
	
	public String getSrcSysCd() {
		return UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD;
	}
	
	public String getDeployNodeNm() {
		return UobConstants.ADOBE_SITE_CATALYST_DEPL_NODE_NM;
	}

	public String getProcCriticalityCd() {
		return UobConstants.ADOBE_SITE_CATALYST_PROC_CRIT_CD;
	}
	
	public String getIsActFlg() {
		return UobConstants.ADOBE_SITE_CATALYST_IS_ACT_FLG;
	}
	
	
}
