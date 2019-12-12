/**
 * 
 */
package com.uob.edag.model;

import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;

/**
 * @author Muhammad Bilal
 *
 */
public class ProcessMaster {
	private String procId;
	private String procNm;
	private String procTypeCd;
	private int procGrpId;
	private String procDesc;
	private String procFreqCd;
	private String srcSysCd;
	private String deployNodeNm;
	private String procCriticalityCd;
	private char isActFlg;
	
	private LoadProcess loadProcess;
	private SourceTableDetail sourceTableDetail;
	
	private List<ProcParam> procParamsList;
	private List<ProcDownstreamAppl> procApplList;
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private static final MessageFormat REMOVE_PROC_DOWNSTREAM_APPL_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_PROC_DOWNSTREAM_APPL WHERE PROC_ID = ''{0}''; "
	);
			
	private static final MessageFormat REMOVE_PROC_PARAM_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_PROC_PARAM WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_FIELD_DETAIL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("DELETE FROM EDAG_FIELD_DETAIL ") +
      UobUtils.ltrim("WHERE FILE_ID IN (SELECT FILE_ID ") +
	    UobUtils.ltrim("                  FROM EDAG_FILE_DETAIL ") +
      UobUtils.ltrim("                  WHERE PROC_ID = ''{0}''); ")
	);
	
	private static final MessageFormat REMOVE_CONTROL_FILE_DETAIL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("DELETE FROM EDAG_CONTROL_FILE_DETAIL ") +
      UobUtils.ltrim("WHERE CTRL_FILE_ID IN (SELECT CTRL_FILE_ID ") +
	    UobUtils.ltrim("                       FROM EDAG_FILE_DETAIL ") +
      UobUtils.ltrim("                       WHERE PROC_ID = ''{0}''); ")
	);
	
	private static final MessageFormat REMOVE_FILE_DETAIL_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_FILE_DETAIL WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_EXPORT_PROCESS_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_EXPORT_PROCESS WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_LOAD_PROCESS_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_LOAD_PROCESS WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_COUNTRY_CONTROL_CHAR_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_PROC_CTRY_CTRL_CHAR WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_PROCESS_COUNTRY_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_PROC_CTRY_DTL WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_PROCESS_MASTER_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_PROCESS_MASTER WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat INSERT_AUDIT_DETAIL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_AUDIT_DETAIL(AUDIT_ID, AUDIT_DT, USR_NM, USR_ACTION_TXT, STATUS_CD) ") +
      UobUtils.ltrim("VALUES(SEQ_AUDIT_ID.NEXTVAL, SYSDATE, ''{0}'', ''{1}'', ''{2}''); ")
	);
	
	private static final MessageFormat INSERT_PROCESS_MASTER_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROCESS_MASTER(PROC_ID, PROC_NM, PROC_TYPE_CD, PROC_GRP_ID, PROC_DESC, ") +
      UobUtils.ltrim("                                PROC_FREQ_CD, PROC_CRITICALITY_CD, DEPLOY_NODE_NM, SRC_SYS_CD, ") +
	    UobUtils.ltrim("                                IS_ACT_FLG, CRT_DT, CRT_USR_NM, UPD_DT, UPD_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', null, ''{6}'', ''{7}'', ''{8}'', DEFAULT, ''{9}'', DEFAULT, ''{10}''); ")
	);
	
	private static final MessageFormat REMOVE_FIELD_STD_RULES_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("DELETE FROM EDAG_FIELD_STD_RULES ") +
      UobUtils.ltrim("WHERE FILE_ID IN (SELECT FILE_ID ") +
      UobUtils.ltrim("                  FROM EDAG_FILE_DETAIL ") +
      UobUtils.ltrim("                  WHERE PROC_ID = ''{0}''); ")
	);
	
	private static final MessageFormat REMOVE_FIELD_NAME_PATTERNS_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("DELETE FROM EDAG_FIELD_NAME_PATTERNS WHERE PROC_ID= ''{0}'';")
	);
	
	private static final MessageFormat REMOVE_ALERT = new MessageFormat(
			"DELETE FROM EDAG_ALERTS WHERE PROC_ID = ''{0}''; "
	);
	
	private static final MessageFormat REMOVE_SOURCE_TABLE_DETAIL_TEMPLATE = new MessageFormat(
			"DELETE FROM EDAG_SOURCE_TABLE_DETAIL WHERE PROC_ID = ''{0}''; "
	);
	
	public String getRemoveSourceTableDetailSql() {
		String result = REMOVE_SOURCE_TABLE_DETAIL_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove source table detail statement: " + result);
		return result;
	}
			
	public String getRemoveAlertsSql() {
		String result = REMOVE_ALERT.format(new Object[] {getProcId()});
		logger.debug("Remove alert statement: " + result);
		return result;
	}
	
	public String getRemoveProcDownstreamApplSql() {
		String result = REMOVE_PROC_DOWNSTREAM_APPL_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove proc downstream appl statement: " + result);
		return result;
	}
	
	public String getRemoveProcParamSql() {
		String result = REMOVE_PROC_PARAM_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove proc param statement: " + result);
		return result;
	}
	
	public String getRemoveFieldStdRulesSql() {
		String result = REMOVE_FIELD_STD_RULES_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove field std rules statement: " + result);
		return result;
	}
	
	public String getRemoveFieldDetailSql() {
		String result = REMOVE_FIELD_DETAIL_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove field detail statement: " + result);
		return result;
	}
	
	public String getRemoveControlFileDetailSql() {
		String result = REMOVE_CONTROL_FILE_DETAIL_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove control file detail statement: " + result);
		return result;
	}
	
	public String getRemoveFileDetailSql() {
		String result = REMOVE_FILE_DETAIL_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove file detail statement: " + result);
		return result;
	}
	
	public String getRemoveExportProcessSql() {
		String result = REMOVE_EXPORT_PROCESS_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove export process statement: " + result);
		return result;
	}
	
	public String getRemoveLoadProcessSql() {
		String result = REMOVE_LOAD_PROCESS_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove load process statement: " + result);
		return result;
	}

	public String getRemoveCountryControlCharSql() {
		String result = REMOVE_COUNTRY_CONTROL_CHAR_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove load process statement: " + result);
		return result;
	}

	public String getRemoveProcessCountrySql() {
		String result = REMOVE_PROCESS_COUNTRY_TEMPLATE.format(new Object[] {this.getProcId()});
		logger.debug("Remove process country statement: " + result);
		return result;
	}
	
	public String getRemoveProcessMasterSql() {
		String result = REMOVE_PROCESS_MASTER_TEMPLATE.format(new Object[] {this.getProcId()});
		logger.debug("Remove process master statement: " + result);
		return result;
	}
	
	public String getInsertAuditDetailSql(String action, String status) {
		String result = INSERT_AUDIT_DETAIL_TEMPLATE.format(new Object[] {UobConstants.SYS_USER_NAME, action, status});
		logger.debug("Insert audit detail statement: " + result);
		return result;
	}
	
	public String getInsertProcessMasterSql() {
		String result = INSERT_PROCESS_MASTER_TEMPLATE.format(new Object[] {getProcId(), getProcNm(), 
				                                                    						getProcTypeCd(), getProcGrpId(),
				                                                    						getProcDesc(), getProcFreqCd(),
				                                                    						getDeployNodeNm(), getSrcSysCd(),
				                                                    						getIsActFlg(), UobConstants.SYS_USER_NAME,
				                                                    						UobConstants.SYS_USER_NAME});
		logger.debug("Insert to process master SQL: " + result);
		return result;
	}
	
	public String getDeleteFieldNamePatternsSql() {
		String result = REMOVE_FIELD_NAME_PATTERNS_TEMPLATE.format(new Object[] {getProcId()});
		logger.debug("Remove Field Name Patterns Statement " + result);
		return result;
	}
	
	public String getProcId() {
		return procId;
	}
	
	public void setProcId(String procId) {
		this.procId = procId == null ? null : procId.trim();
	}
	
	public String getProcNm() {
		return procNm;
	}
	
	public void setProcNm(String procNm) {
		this.procNm = procNm == null ? null : procNm.trim();
	}
	
	public String getProcTypeCd() {
		return procTypeCd;
	}
	
	public void setProcTypeCd(String procTypeCd) {
		this.procTypeCd = procTypeCd == null ? null : procTypeCd.trim();
	}
	
	public int getProcGrpId() {
		return procGrpId;
	}
	
	public void setProcGrpId(int procGrpId) {
		this.procGrpId = procGrpId;
	}
	
	public String getProcDesc() {
		return procDesc;
	}
	
	public String getNormalizedProcDesc() {
		String result = getProcDesc();
		
		if (result != null) {
			result = result.replaceAll(UobConstants.QUOTE, UobConstants.EMPTY)
										 .replaceAll(UobConstants.NEWLINE, UobConstants.SPACE)
										 .replace(UobConstants.BACKSLASH, UobConstants.SPACE)
										 .replaceAll(UobConstants.CR, UobConstants.SPACE);
		}
		
		return result;
	}
	
	public void setProcDesc(String procDesc) {
		this.procDesc = procDesc == null ? null : procDesc.trim();
	}
	
	public String getProcFreqCd() {
		return procFreqCd;
	}
	
	public void setProcFreqCd(String procFreqCd) {
		this.procFreqCd = procFreqCd == null ? null : procFreqCd.trim();
	}
	
	public String getSrcSysCd() {
		return srcSysCd;
	}
	
	public void setSrcSysCd(String srcSysCd) {
		this.srcSysCd = srcSysCd == null ? null : srcSysCd.trim();
	}
	
	public String getDeployNodeNm() {
		return deployNodeNm;
	}
	
	public void setDeployNodeNm(String deployNodeNm) {
		this.deployNodeNm = deployNodeNm ==  null ? null : deployNodeNm.trim();
	}
	
	public String getProcCriticalityCd() {
		return procCriticalityCd;
	}
	
	public void setProcCriticalityCd(String procCriticalityCd) {
		this.procCriticalityCd = procCriticalityCd == null ? null : procCriticalityCd.trim();
	}
	
	public char getIsActFlg() {
		return isActFlg;
	}
	
	public void setIsActFlg(char isActFlg) {
		this.isActFlg = isActFlg;
	}
	
	public LoadProcess getLoadProcess() {
		return loadProcess;
	}
	
	public void setLoadProcess(LoadProcess loadProcess) {
		this.loadProcess = loadProcess;
	}
	
	public SourceTableDetail getSourceTableDetail() {
		return sourceTableDetail;
	}
	
	public void setSourceTableDetail(SourceTableDetail sourceTableDetail) {
		this.sourceTableDetail = sourceTableDetail;
	}
	
	public List<ProcParam> getProcParamsList() {
		return procParamsList;
	}
	
	public void setProcParamsList(List<ProcParam> procParamsList) {
		this.procParamsList = procParamsList;
	}
	
	public List<ProcDownstreamAppl> getProcApplList() {
		return procApplList;
	}
	
	public void setProcApplList(List<ProcDownstreamAppl> procApplList) {
		this.procApplList = procApplList;
	}
	
	public String toString() {
		return new ToStringBuilder(this).append("procId", this.procId)
				                            .append("procGrpId", this.procGrpId)
				                            .append("procNm", this.procNm)
				                            .append("procTypeCd", this.procTypeCd)
				                            .append("deployNodeNm", this.deployNodeNm)
				                            .append("isActFlg", this.isActFlg)
				                            .append("procCriticalityCd", this.procCriticalityCd)
				                            .append("procDesc", this.procDesc)
				                            .append("procFreqCd", this.procFreqCd)
				                            .append("srcSysCd", this.srcSysCd)
				                            .toString();
	}
}
