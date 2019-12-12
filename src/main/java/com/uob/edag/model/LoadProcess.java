/**
 * 
 */
package com.uob.edag.model;

import java.text.MessageFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;

/**
 * @author Muhammad Bilal
 *
 */
public class LoadProcess {
	
	private static final MessageFormat INSERT_LOAD_PROCESS_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_LOAD_PROCESS(PROC_ID, TGT_COMPR_TYPE_CD, TGT_DB_NM, TGT_TBL_NM, TGT_TBL_PART_TXT, ") +
      UobUtils.ltrim("                              TGT_APLY_TYPE_CD, STG_DB_NM, STG_TBL_NM, STG_TBL_PART_TXT, ") +
      UobUtils.ltrim("                              ERR_THRESHOLD, CRT_DT, CRT_USR_NM, UPD_DT, UPD_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', null, ''{4}'', ''{5}'', ''{6}'', null, ''{7}'', DEFAULT, ''{8}'', DEFAULT, ''{8}''); ")
  );
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private String procId;
	private String tgtDirNm;
	private String tgtFormatCd;
	private String tgtComprTypeCd;
	private String tgtAplyTypeCd;
	private String tgtDbNm;
	private String tgtTblNm;
	private String tgtTblPartTxt;
	private String stgDirNm;
	private String stgDbNm;
	private String stgTblNm;
	private String stgTblPartTxt;
	private double errThreshold;
	
	public LoadProcess() {
		// default constructor
	}
	
	public LoadProcess(HadoopModel model) {
		this.procId = model.getProcessId();
		this.tgtComprTypeCd = model.getHadoopCompressCd();
		this.tgtDbNm = model.getHiveDbName();
		this.tgtTblNm = model.getHiveTableName();
		this.tgtTblPartTxt = model.getHivePartition();
		this.tgtAplyTypeCd = model.getLoadTypeCd();
		this.stgDbNm = model.getStagingDbName();
		this.stgTblNm = model.getStagingTableName();
		this.stgTblPartTxt = model.getStagingHivePartition();
		this.errThreshold = model.getHiveErrorThreshold();
	}
	
	public String getInsertLoadProcessSql() {
		String result = INSERT_LOAD_PROCESS_TEMPLATE.format(new Object[] {getProcId(), getTgtComprTypeCd(),
																														 					getTgtDbNm(), getTgtTblNm(),
																														 					getTgtAplyTypeCd(), getStgDbNm(),
																														 					getStgTblNm(), getErrThreshold(),
																														 					UobConstants.SYS_USER_NAME});
		logger.debug("Insert load process statement: " + result);
		return result;
	}

	public String getProcId() {
		return procId;
	}

	public void setProcId(String procId) {
		this.procId = procId == null ? null : procId.trim();
	}

	public String getTgtDirNm() {
		return tgtDirNm;
	}

	public void setTgtDirNm(String tgtDirNm) {
		this.tgtDirNm = tgtDirNm == null ? null : tgtDirNm;
	}

	public String getTgtFormatCd() {
		return tgtFormatCd;
	}

	public void setTgtFormatCd(String tgtFormatCd) {
		this.tgtFormatCd = tgtFormatCd == null ? null : tgtFormatCd;
	}

	public String getTgtComprTypeCd() {
		return tgtComprTypeCd;
	}

	public void setTgtComprTypeCd(String tgtComprTypeCd) {
		this.tgtComprTypeCd = tgtComprTypeCd == null ? null : tgtComprTypeCd.trim();
	}

	public String getTgtAplyTypeCd() {
		return tgtAplyTypeCd;
	}

	public void setTgtAplyTypeCd(String tgtAplyTypeCd) {
		this.tgtAplyTypeCd = tgtAplyTypeCd == null ? null : tgtAplyTypeCd.trim();
	}

	public String getTgtDbNm() {
		return tgtDbNm;
	}

	public void setTgtDbNm(String tgtDbNm) {
		this.tgtDbNm = tgtDbNm == null ? null : tgtDbNm.trim();
	}

	public String getTgtTblNm() {
		return getTgtTblNm(false);
	}
	
	public String getTgtTblNm(boolean withSchema) {
		return (withSchema && !StringUtils.isEmpty(tgtDbNm)) ? tgtDbNm + "." + tgtTblNm : tgtTblNm;
	}

	public void setTgtTblNm(String tgtTblNm) {
		this.tgtTblNm = tgtTblNm == null ? null : tgtTblNm.trim();
	}

	public String getTgtTblPartTxt() {
		return tgtTblPartTxt;
	}

	public void setTgtTblPartTxt(String tgtTblPartTxt) {
		this.tgtTblPartTxt = tgtTblPartTxt == null ? null : tgtTblPartTxt.trim();
	}

	public String getStgDirNm() {
		return stgDirNm;
	}

	public void setStgDirNm(String stgDirNm) {
		this.stgDirNm = stgDirNm == null ? null : stgDirNm.trim();
	}

	public String getStgDbNm() {
		return stgDbNm;
	}

	public void setStgDbNm(String stgDbNm) {
		this.stgDbNm = stgDbNm == null ? null : stgDbNm.trim();
	}

	public String getStgTblNm() {
		return stgTblNm;
	}

	public void setStgTblNm(String stgTblNm) {
		this.stgTblNm = stgTblNm == null ? null : stgTblNm.trim();
	}

	public String getStgTblPartTxt() {
		return stgTblPartTxt;
	}

	public void setStgTblPartTxt(String stgTblPartTxt) {
		this.stgTblPartTxt = stgTblPartTxt == null ? null : stgTblPartTxt.trim();
	}

	public double getErrThreshold() {
		return errThreshold;
	}

	public void setErrThreshold(double errThreshold) {
		this.errThreshold = errThreshold;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("procId", procId)
				                            .append("tgtDirNm", tgtDirNm)
				                            .append("tgtFormatCd", tgtFormatCd)
				                            .append("tgtComprTypeCd", tgtComprTypeCd)
				                            .append("tgtAplyTypeCd", tgtAplyTypeCd)
				                            .append("tgtDbNm", tgtDbNm)
				                            .append("tgtTblNm", tgtTblNm)
				                            .append("tgtTblPartTxt", tgtTblPartTxt)
				                            .append("stgDirNm", stgDirNm)
				                            .append("stgDbNm", stgDbNm)
				                            .append("stgTblNm", stgTblNm)
				                            .append("stgTblPartTxt", stgTblPartTxt)
				                            .append("errThreshold", errThreshold)
				                            .toString();
	}
}
