package com.uob.edag.model;

import java.text.MessageFormat;

import com.uob.edag.utils.UobUtils;

public interface ExportModel extends TargetModel {
	
	public static final MessageFormat INSERT_EXPORT_PROCESS_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_EXPORT_PROCESS(PROC_ID, SRC_DB_NM, SRC_TBL_NM, TGT_DIR_NM, TGT_FILE_NM, ") +
      UobUtils.ltrim("                                TGT_FILE_EXTN_NM, TGT_FILE_COL_DELIM_TXT, TGT_FILE_TXT_DELIM_TXT, ") +
      UobUtils.ltrim("                                CTL_FILE_NM, CTL_FILE_EXTN_NM, CRT_DT, CRT_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''{9}'', DEFAULT, ''{10}''); ")
	);
	
	String getInsertExportProcessSql();
	String getSrcDbName();
	void setSrcDbName(String srcDbName);
	String getSrcTblName();
	void setSrcTblName(String srcTblName);
	String getTgtDirName();
	void setTgtDirName(String tgtDirName);
	String getTgtFileName();
	void setTgtFileName(String tgtFileName);
	String getTgtFileExtn();
	void setTgtFileExtn(String tgtFileExtn);
	String getTgtColDelim();
	void setTgtColDelim(String tgtColDelim);
	String getTgtTxtDelim();
	void setTgtTxtDelim(String tgtTxtDelim);
	String getCtrlFileName();
	void setCtrlFileName(String ctrlFileName);
	String getCtrlFileExtn();
	void setCtrlFileExtn(String ctrlFileExtn);
}
