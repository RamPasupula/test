package com.uob.edag.model;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file defines the format of a process. Process is the high level 
 *                  abstraction of one execution unit. Process to file mapping is one to one.
 * 
 */

public class ProcessModel extends ProcessMaster {
	
	public static final String DEFAULT_REFERENCED_FILE_SOURCE_FOLDER = PropertyLoader.getProperty("ReferencedFile.Source.Folder");
	
	public static final String DEFAULT_HDFS_TARGE_FOLDER = PropertyLoader.getProperty("ReferencedFile.Target.Folder");
	
	public static final String DEFAULT_HDFS_PATH_SUFFIX = PropertyLoader.getProperty("ReferencedFile.URIField.Suffix");
	
	private static String COUNTRY_COUNTRL_CHAR = "[\\u0000\\u000D\\u000E\\u000F\\u00A0\\u0085]";
	
	private static final MessageFormat INSERT_PROC_DOWNSTREAM_APPL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROC_DOWNSTREAM_APPL(PROC_ID, APPL_CD, CRT_DT, CRT_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', SYSDATE, ''{2}''); ")
  );
	
	private static final MessageFormat INSERT_PROC_PARAM_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROC_PARAM(PROC_ID, PARAM_NM, PARAM_VAL, CRT_DT, CRT_USR_NM) ") + 
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', DEFAULT, ''{3}''); ")
	);
	
	private static final MessageFormat INSERT_PROCESS_MASTER_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_PROCESS_MASTER(PROC_ID, PROC_NM, PROC_TYPE_CD, PROC_GRP_ID, ") + 
      UobUtils.ltrim("                                PROC_DESC, PROC_FREQ_CD, PROC_CRITICALITY_CD, DEPLOY_NODE_NM, SRC_SYS_CD, IS_ACT_FLG, ") + 
      UobUtils.ltrim("                                CRT_DT, CRT_USR_NM) ") +
      UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}'', ''{7}'', ''{8}'', ''Y'', DEFAULT, ''{9}''); ")
	);
	
	private static final MessageFormat INSERT_PROCESS_COUNTRY_TEMPLATE = new MessageFormat(
			"INSERT INTO EDAG_PROC_CTRY_DTL(PROC_ID, CTRY_CD, IS_ACT_FLG, CHARSET, ENCODING, REFERENCED_FILE_FOLDER, UPD_DT, UPD_USR_NM) " +
      "VALUES(''{0}'', ''{1}'', ''Y'', ''{2}'', ''{3}'', {5}, DEFAULT, ''{4}''); "
	);
	
	
	private static final MessageFormat INSERT_COUNTRY_CONTROL_CHAR_TEMPLATE = new MessageFormat(
			"INSERT INTO EDAG_PROC_CTRY_CTRL_CHAR (PROC_ID, CTRY_CD, CTRL_CHAR_PATTERN, REPLACEMENT_CHARS, CRT_DT, CRT_USR_NM) " +
	"VALUES (''{0}'', ''{1}'', ''{2}'', '' '', DEFAULT, DEFAULT);"
	);
	

	public String getInsertProcDownstreamApplSql() {
		String result = INSERT_PROC_DOWNSTREAM_APPL_TEMPLATE.format(new Object[] {getProcId(), getDownstreamAppl(),
    																																				  UobConstants.SYS_USER_NAME});
		logger.debug("Insert proc downstream appl statement: " + result);
		return result;
	}
	
	/**
   * This method is used to generate the SQL to insert an entry into the Process Master table.
   * @return The SQL statement
   */
  public String getInsertProcessMasterSql() {
  	String result = INSERT_PROCESS_MASTER_TEMPLATE.format(new Object[] {getProcId(), getProcNm(), 
  			                                                                this.getProcTypeCd(), this.getProcGrpId(),
  			                                                                this.getProcDesc(), this.getProcFreqCd(),
  			                                                                this.procPriority, this.getDeployNodeNm(),
  			                                                                this.getSrcSysCd(), this.userNm});
  	logger.debug("Insert process master SQL: " + result);
  	return result;
  }
  
  public FileModel getSrcInfo() {
    return srcInfo;
  }
  
  public void setSrcInfo(FileModel srcInfo) {
    this.srcInfo = srcInfo;
  }
  
  public DestModel getDestInfo() {
    return destInfo;
  }
  
  public void setDestInfo(DestModel destInfo) {
    this.destInfo = destInfo;
  }
  
  public AlertModel getAlertInfo() {
    return alertInfo;
  }
  
  public void setAlertInfo(AlertModel alertInfo) {
    this.alertInfo = alertInfo;
  }
  
  public String getUserNm() {
    return userNm;
  }
  
  public void setUserNm(String userNm) {
    this.userNm = userNm == null ? null : userNm.trim();
  }
  
  public String getProcParam(String paramName) {
  	if (procParam != null) {
  		for (ProcessParam param : procParam) {
  			if (paramName.equals(param.getParamName())) {
  				return param.getParamValue();
  			}
  		}
  	}
  	
  	return null;
  }
  
  public List<ProcessParam> getProcParam() {
    return procParam;
  }
  
  public void setProcParam(List<ProcessParam> procParam) {
    this.procParam = procParam;
  }
  
  public String getProcPriority() {
    return procPriority;
  }

  public void setProcPriority(String procPriority) {
    this.procPriority = procPriority == null ? null : procPriority.trim();
  }
  
  public String getDownstreamAppl() {
    return downstreamAppl;
  }

  public void setDownstreamAppl(String downstreamAppl) {
    this.downstreamAppl = downstreamAppl == null ? null : downstreamAppl.trim();
  }
  
  public List<String> getInsertProcessCountrySql() {
  	List<String> result = new ArrayList<String>();
  	
  	for (CountryAttributes countryAttributes : this.getAllCountryAttributes()) {
  		String refFileFolder = countryAttributes.getReferencedFileFolder() == null ? "null" : "'" + countryAttributes.getReferencedFileFolder() + "'";
  		result.add(INSERT_PROCESS_COUNTRY_TEMPLATE.format(new Object[] {getProcId(), countryAttributes.getCountryCode(),
  																																	  countryAttributes.getCharset(),
  																																	  countryAttributes.getEncoding(), getUserNm(),
  																																	  refFileFolder}));
  	}
  	
  	logger.debug("Process country insert statements: " + result);
  	return result;
  }
  
  public List<String> getInsertCountryControlCharacter() {
    List<String> result = new ArrayList<>();
	String countryControlChar = StringUtils.trimToNull(PropertyLoader.getProperty(UobConstants.COUNTRY_COUNTRL_CHAR)) == null ? COUNTRY_COUNTRL_CHAR
			:PropertyLoader.getProperty(UobConstants.COUNTRY_COUNTRL_CHAR);

	for (CountryAttributes countryAttributes : this.getAllCountryAttributes()) {
	  	result.add(INSERT_COUNTRY_CONTROL_CHAR_TEMPLATE.format(new Object[] {getProcId(), countryAttributes.getCountryCode(), countryControlChar }));
	}
    
    logger.debug("Process country control char insert statements: " + result);
	return result;
	  
  }
  
  public List<String> getInsertProcParamSql() {
  	List<String> result = new ArrayList<String>();
  	
  	for (ProcessParam param : this.getProcParam()) {
  		result.add(INSERT_PROC_PARAM_TEMPLATE.format(new Object[] {getProcId(), param.getParamName(), 
  																															 param.getParamValue(), UobConstants.SYS_USER_NAME}));
  	}
  	
  	logger.debug("Insert proc param statements: " + result);
  	return result;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).appendSuper(super.toString())
    		                            .append("procPriority", procPriority)
    		                            .append("countryAttributesMap", countryAttributesMap)
    		                            .append("srcInfo", srcInfo)
    		                            .append("destInfo", destInfo)
    		                            .append("alertInfo", alertInfo)
    		                            .append("userNm", userNm)
    		                            .append("procParam", procParam)
                                    .append("downstreamAppl", downstreamAppl)
                                    .toString();
  }
  
  // *** to maintain compatibility with mybatis mapping
  
  public String getDeployNode() {
  	return this.getDeployNodeNm();
  }
  
  public void setDeployNode(String d) {
  	this.setDeployNodeNm(d);
  }
  
  public String getProcName() {
  	return this.getProcNm();
  }
  
  public void setProcName(String p) {
  	this.setProcNm(p);
  }
  
  public int getProcGroupId() {
  	return this.getProcGrpId();
  }
  
  public void setProcGroupId(int id) {
  	this.setProcGrpId(id);
  }
  
  public String getProcFreq() {
  	return this.getProcFreqCd();
  }
  
  public void setProcFreq(String f) {
  	this.setProcFreqCd(f);
  }
  
  public String getSrcSystemId() {
  	return this.getSrcSysCd();
  }
  
  public void setSrcSystemId(String id) {
  	this.setSrcSysCd(id);
  }
  
  public CountryAttributes addCountry(String countryCode) {
  	CountryAttributes attr = new CountryAttributes(countryCode);
  	countryAttributesMap.put(countryCode, attr);

  	return attr;
  }
  
  public void setCountryAttributesMap(Map<String, CountryAttributes> map) {
  	this.countryAttributesMap = map;
  }
  
  public Map<String, CountryAttributes> getCountryAttributesMap() {
  	return this.countryAttributesMap;
  }
  
  public Collection<CountryAttributes> getAllCountryAttributes() {
  	return countryAttributesMap.values();
  }
  
  public Set<String> getCountryCodes() {
  	return countryAttributesMap.keySet();
  }
  
  // *** end of mybatis mapping compatibility
  
  private String procPriority;   // Priority of the Process
  private Map<String, CountryAttributes> countryAttributesMap = new HashMap<String, CountryAttributes>();  // Country Codes of the process
  private FileModel srcInfo;      // Source Information - File, Database,etc
  private DestModel destInfo;    // Hadoop Information
  private AlertModel alertInfo;    // Alert Information
  private String userNm;        // User
  private List<ProcessParam> procParam;  // Character Set of the Process
  private String downstreamAppl;  // Downstream Application
}
