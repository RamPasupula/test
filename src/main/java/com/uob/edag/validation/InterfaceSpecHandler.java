package com.uob.edag.validation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.RegistrationDao;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.AlertModel;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FieldModel.RecordType;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.InterfaceSpec;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.RuleModel;
import com.uob.edag.utils.InterfaceSpecMap;
import com.uob.edag.utils.PropertyLoader;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for validating the Interface Spec details.
 *              Only if the validation passes, the details will be registered
 *              into the database.
 * 
 */

public class InterfaceSpecHandler {
	
	public static final String MAX_RECORD_LENGTH = "MAX_RECORD_LENGTH";
	public static final String MAX_FILE_REFERENCES = "referencedfile.max.references.in.file";
	public static final int DEFAULT_MAX_FILE_REFERENCES = 10;
	
	private static final String FILE_FORMAT_PREFIX = InterfaceSpecHandler.class.getName() + ".FileFormat.";
	private static final String STRING_ENCLOSURE_PREFIX = InterfaceSpecHandler.class.getName() + ".StringEnclosure.";
	private static final String FIELD_DELIMITER_PREFIX = InterfaceSpecHandler.class.getName() + ".FieldDelimiter.";
	
	private static Map<String, FileLayout> VALID_FILE_LAYOUTS = new HashMap<String, FileLayout>();
	private static Map<String, String> LOAD_TYPE_MAP = new HashMap<String, String>();
	
	static {
		try {
			for (String key : PropertyLoader.getAllPropertyKeys()) {
				if (key.startsWith(FILE_FORMAT_PREFIX) || key.startsWith(STRING_ENCLOSURE_PREFIX) || key.startsWith(FIELD_DELIMITER_PREFIX)) {
					String layoutKey = key.substring(key.lastIndexOf(".") + 1);
					FileLayout fileLayout = VALID_FILE_LAYOUTS.get(layoutKey);
					if (fileLayout == null) {
						fileLayout = new FileLayout();
						VALID_FILE_LAYOUTS.put(layoutKey, fileLayout);
					}
					
					String value = PropertyLoader.getProperty(key);
					if (key.startsWith(FILE_FORMAT_PREFIX)) {
						fileLayout.fileFormat = value;
					} else if (key.startsWith(FIELD_DELIMITER_PREFIX)) {
						fileLayout.fieldDelimiter = value;
					} else {
						fileLayout.stringEnclosure = value;
					}
				}
			}
		} catch (EDAGIOException e) {
			throw new RuntimeException("Unable to get all property keys: " + e.getMessage());
		}
		
		LOAD_TYPE_MAP.put(UobConstants.FULL_LOAD.toUpperCase(), UobConstants.FULL_LOAD_CD);
		LOAD_TYPE_MAP.put(UobConstants.APPEND_LOAD.toUpperCase(), UobConstants.APPEND_LOAD_CD);
		LOAD_TYPE_MAP.put(UobConstants.HISTORY_LOAD.toUpperCase(), UobConstants.HISTORY_LOAD_CD);
		LOAD_TYPE_MAP.put(UobConstants.ADDITIONAL_LOAD.toUpperCase(), UobConstants.ADDITIONAL_LOAD_CD);
	}
	
	private static class FileLayout {
		private String stringEnclosure;
		private String fileFormat;
		private String fieldDelimiter;
	}
	
  protected Logger logger = Logger.getLogger(getClass());

  /**
   * This method is used to transform the Interface Specification into a list of
   *     Process Model objects.
   * @param spec The Interface Spec object
   * @param force Force flag
   * @param filesToForce List of files to be force processed
   * @param filesToProcess List of files to be processed
   * @return a List of Process Model objects
   * @throws Exception when there is an error transforming the Interface Specification
   *     into the Process Model.
   */
  public ProcessModel handleInterfaceSpecDetails(InterfaceSpec spec, boolean force, String filesToForce, 
  		                                           String filesToProcess, String fileName, 
  		                                           InterfaceSpecMap srcFileSpec) throws EDAGMyBatisException, EDAGValidationException {

    List<String> forceFilesList = null;
    boolean forceAll = force && "*".equalsIgnoreCase(filesToForce);
    if (force && !forceAll && StringUtils.isNotEmpty(filesToForce)) {
      String[] forceFilesArr = filesToForce.split(UobConstants.COMMA);
      forceFilesList = Arrays.asList(forceFilesArr);
      logger.debug("Force Files List is: " + Arrays.toString(forceFilesList.toArray()));
    }

    RegistrationDao regisDao = new RegistrationDao();
    List<RuleModel> stdList;
		stdList = regisDao.retrieveStdRules();

    String controlFileNm = null;
    List<FieldModel> ctrlFieldInfo = null;
    boolean containsControl = false;
    InterfaceSpecMap ctrlfileSpec = spec.getCtrlFileSpec();

    if (ctrlfileSpec != null) {
      containsControl = true;
      controlFileNm = ctrlfileSpec.get(UobConstants.FILE_NM, String.class);
      logger.debug("Interface contains separate Control File: " + controlFileNm);
      ctrlFieldInfo = new ArrayList<FieldModel>();
    }

    logger.debug("Going to validate spec for: " + fileName);

    /// File Name - Non Blank
    if (StringUtils.isEmpty(fileName)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE,  "File Name", "File Name in Interface Spec is empty");
    }
      
    // File Frequency
    String fileFrequency = StringUtils.trimToEmpty(srcFileSpec.get(UobConstants.FILE_FREQ, String.class));
    boolean validFileFreq = UobConstants.VALID_RECURRENCES.contains(fileFrequency);
    if (!validFileFreq) {
      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, fileFrequency, "Invalid File Frequency for file: " + fileName);
    }
      
    String frequency = "";
    switch (fileFrequency) {
      case UobConstants.DAILY:
        frequency = UobConstants.DAILY_CD;
        break;
      case UobConstants.WEEKLY:
        frequency = UobConstants.WEEKLY_CD;
        break;
      case UobConstants.MONTHLY:
        frequency = UobConstants.MONTHLY_CD;
        break;
      case UobConstants.QUARTERLY:
        frequency = UobConstants.QUARTERLY_CD;
        break;
      case UobConstants.YEARLY:
        frequency = UobConstants.YEARLY_CD;
        break;
      case UobConstants.ADHOC:
        frequency = UobConstants.ADHOC_CD;
        break;
      case UobConstants.CYCLIC:
    	  frequency = UobConstants.CYCLIC_CD;
    	  break;
      default:
        break;
    }

    Map<String, Map<String, InterfaceSpecMap>> procSpecList = spec.getProcessSpec();
    if (!procSpecList.containsKey(spec.getInterfaceSpecName())) {
      throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Process Specification", "Process Specifications for the given interface spec is missing");
    }
    
    Map<String, InterfaceSpecMap> procSpecListPerSpec = procSpecList.get(spec.getInterfaceSpecName());
    if (!procSpecListPerSpec.containsKey(fileName)) {
      throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, fileName, fileName + " is missing in Process Specification");
    }
    
    InterfaceSpecMap procSpec = procSpecListPerSpec.get(fileName);

    // Process ID Validation
    String processId = StringUtils.trimToNull(procSpec.get(UobConstants.PROCESS_ID, String.class));
    if (processId == null) {
      throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Process ID", "Process ID cannot be null");
    }

    if (StringUtils.isNotEmpty(filesToProcess) && !"*".equalsIgnoreCase(filesToProcess)) {
      String[] filesToProcessArr = filesToProcess.split(UobConstants.COMMA);
      boolean process = false;
      for (String fileProc : filesToProcessArr) {
        if (fileProc.trim().equalsIgnoreCase(processId)) {
          logger.debug("Proc Spec for proc id: " + processId + " is: " + procSpec);
          process = true;
        }
      }
      
      if (!process) {
        logger.debug("File is not in the process list: " + processId);
        return null;
      }
    }
    
    // File Name - Unique
    if (!forceAll && (!force || forceFilesList == null || !forceFilesList.contains(processId))) {
			if (regisDao.checkFileExists(processId)) {
			  throw new EDAGValidationException("Process already exists: " + processId);
			}
    }

    // Source File Types
    String srcFileType = StringUtils.trimToEmpty(srcFileSpec.get(UobConstants.FILE_TYPE, String.class));
    boolean validSrcFileType = UobConstants.VALID_SRC_FILE_TYPES.contains(srcFileType);
    if (!validSrcFileType) {
      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, srcFileType, "Invalid Source File Type: " + srcFileType + " for file: " + fileName);
    }
    
    String procCrit = null;
    String deployNode = null;
    if (procSpec != null) {
    	String procSrcSysName = StringUtils.trimToEmpty(procSpec.get(UobConstants.SRC_SYS_NM, String.class));
      String inputSrcSysName = spec.getSrcSystem();
      if (!StringUtils.equalsIgnoreCase(procSrcSysName, inputSrcSysName)) {
        throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, inputSrcSysName, "Mismatch in Source System Name, Proc Spec: " + procSrcSysName +
        		                   					  																												", Interface Spec: " + inputSrcSysName);
      }
  
      // Process Name Validation
      String processNm = StringUtils.trimToNull(procSpec.get(UobConstants.PROCESS_NAME, String.class));
      if (processNm == null) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Name");
      }
  
      // Process Group Validation
      String processGrpNm = StringUtils.trimToNull(procSpec.get(UobConstants.PROCESS_GRP, String.class));
      if (processGrpNm == null) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Group is Empty");
      }
  
      // Process Criticality Validation
      procCrit = StringUtils.trimToNull(procSpec.get(UobConstants.PROCESS_CRITICALITY, String.class));
      if (procCrit == null) {
        logger.debug("Process Criticality is Empty for Process: " + processNm + "; Applying Low Criticality");
        procCrit = UobConstants.MED_PRI;
      }
  
      boolean validCrit = UobConstants.VALID_PRI.contains(procCrit);
      if (!validCrit) {
        throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, procCrit, "Invalid Criticality for file: " + fileName);
      }
  
      // Deployment Edge Node Validation
      deployNode = StringUtils.trimToNull(procSpec.get(UobConstants.DEPLOYMENT_NODE, String.class));
      if (deployNode == null) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Deployment Node");
      }
    }
    
    String fileDesc = StringUtils.trimToEmpty(srcFileSpec.get(UobConstants.FILE_DESC, String.class));
    if (fileDesc.length() > 2000) {
      fileDesc = fileDesc.substring(0, 1999); // Handling scenarios where description exceeds 1999 characters
    }

    // Load Strategy
    String loadStrategy = StringUtils.trimToEmpty(srcFileSpec.get(UobConstants.LOAD_STRTGY, String.class));
    boolean validLoadStrategy = UobConstants.VALID_LOAD_STRGY.contains(loadStrategy);
    if (!validLoadStrategy) {
      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, loadStrategy, "Invalid Load Strategy for file: " + fileName);
    }

    // Source File Formats
    String srcFileFormat = StringUtils.trimToEmpty(srcFileSpec.get(UobConstants.FILE_FORMAT, String.class));
    boolean validSrcFileFormat = VALID_FILE_LAYOUTS.keySet().contains(srcFileFormat);
    if (!validSrcFileFormat) {
      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, srcFileFormat, "Invalid Source File Format for file: " + fileName);
    }

    // Decimal Indicator
    String decimalInd = StringUtils.trimToEmpty(srcFileSpec.get(UobConstants.DEC_IND, String.class));
    boolean validDecimalInd = UobConstants.VALID_DEC_IND.contains(decimalInd);
    if (!validDecimalInd) {
      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, decimalInd, "Invalid Decimal Indicator for file: " + fileName);
    }

    // Validate Field Definitions Available
    Map<String, Map<String, InterfaceSpecMap>> srcFieldSpecList = spec.getSrcFieldSpec();
    Map<String, InterfaceSpecMap> srcFieldSpec = srcFieldSpecList.get(fileName);
    if (srcFieldSpec == null || srcFieldSpec.isEmpty()) {
      throw new EDAGValidationException("Field Definitions missing for file: " + fileName);
    }

    boolean containsHeader = false;
    boolean containsFooter = false;

    ProcessModel procModel = new ProcessModel();

    procModel.setProcNm(processId);
    procModel.setProcTypeCd(UobConstants.FILE_INGEST_PROC_TYPE);
    fileDesc = fileDesc.replaceAll(UobConstants.QUOTE, UobConstants.EMPTY);
    fileDesc = fileDesc.replaceAll(UobConstants.SEMICOLON, UobConstants.COMMA);
    fileDesc = fileDesc.replaceAll(UobConstants.ATSIGN, UobConstants.SPACE);
    procModel.setProcDesc(fileDesc);
    procModel.setProcId(processId);
    procModel.setProcPriority(procCrit);
    
    // Process Group Validation
    String procGrpId = StringUtils.trimToNull(procSpec.get(UobConstants.PROCESS_GRP, String.class));
    procModel.setProcGrpId(Double.valueOf(procGrpId).intValue());
    procModel.setProcFreqCd(frequency);

    procModel.setSrcSysCd(spec.getSrcSystem());
    procModel.setUserNm(UobConstants.SYS_USER_NAME);
    procModel.setDeployNodeNm(deployNode);

    for (String countryCode : UobConstants.COUNTRY_CODES) {
    	addCountryIfApplicable(countryCode, procModel, srcFileSpec);
    }
    
    if (procModel.getAllCountryAttributes().isEmpty()) {
      throw new EDAGValidationException("File is not applicable for any country. Please check");
    }

    List<ProcessParam> paramList = new ArrayList<ProcessParam>();
    
    String charset = srcFileSpec.get(UobConstants.CHARSET, String.class);
    if (StringUtils.isNotEmpty(charset)) {
      ProcessParam param = new ProcessParam();
      param.setParamName(UobConstants.CHARSET);
      param.setParamValue(charset);
      paramList.add(param);
    }
    
    // Params
    Map<String, Map<String, String>> paramSpec = spec.getParamSpec();
    Map<String, String> downstreamSpec = spec.getDownstreamSpec();
    if (paramSpec != null && paramSpec.containsKey(processId)) {
      Map<String, String> procParamList = paramSpec.get(processId);
      if (procParamList != null) {
        for (Entry<String, String> entry : procParamList.entrySet()) {
          ProcessParam param = new ProcessParam();
          param.setParamName(entry.getKey());
          param.setParamValue(entry.getValue());
          paramList.add(param);
        }
      }
    }

    procModel.setProcParam(paramList);
    
    // EDF-209
    String frequencyOverride = procModel.getProcParam(UobConstants.PROC_PARAM_PROCESS_FREQUENCY);
    if (UobConstants.HISTORY.equalsIgnoreCase(frequencyOverride)) {
    	procModel.setProcFreqCd(UobConstants.HISTORY_CD);
    	logger.debug("Process " + processId + " frequency is overridden to " + frequencyOverride);
    };
    
    // Set Downstream Application
    if (downstreamSpec != null && downstreamSpec.containsKey(processId)) {
      String downstreamAppName = downstreamSpec.get(processId);
      if (StringUtils.isNotBlank(downstreamAppName)) {
        procModel.setDownstreamAppl(downstreamAppName);
      }
    }

    FileModel srcInfo = new FileModel();
    srcInfo.setSourceFileName(fileName);

    String environment = PropertyLoader.getProperty(UobConstants.ENVIRONMENT);
    String environmentNum = PropertyLoader.getProperty(UobConstants.ENVIRONMENT_NUM);
    
    String landingAreaDirPattern = PropertyLoader.getProperty(UobConstants.LANDING_AREA_DIR_PATH);
    String sourceDirectory = landingAreaDirPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
                                                  .replace(UobConstants.ENV_PARAM, environment)
                                                  .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
                                                  .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
                                                  .replace(UobConstants.FILENM_PARAM, fileName);
    srcInfo.setSourceDirectory(sourceDirectory);
    
    if(procModel.getSrcSysCd().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD))
    	srcInfo.setSourceFileExtn(UobConstants.ADOBE_SITE_CATALYST_FILE_EXTN);
    else
    	srcInfo.setSourceFileExtn(UobConstants.EMPTY);
    String sourceFileTypeId = setFileType(srcFileType);
    srcInfo.setSourceFileTypeId(sourceFileTypeId);

    srcInfo = setFileLayout(srcFileFormat, srcInfo);
    
    srcInfo.setExplicitDecimalPoint(decimalInd.equalsIgnoreCase(UobConstants.EXPLICIT) ? UobConstants.Y : UobConstants.N);

    String srcArchiveDirPattern = PropertyLoader.getProperty(UobConstants.LANDING_AREA_ARCHIVE_DIR_PATH);
    String sourceArchivalDir = srcArchiveDirPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
        																					 .replace(UobConstants.ENV_PARAM, environment)
        																					 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
        																					 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
        																					 .replace(UobConstants.FILENM_PARAM, fileName);
    srcInfo.setSourceArchivalDir(sourceArchivalDir);

    if (containsControl) {
      srcInfo.setControlInfo(UobConstants.CTRL_INFO_C);

      String controlFileDir = landingAreaDirPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd().toLowerCase())
          																				 .replace(UobConstants.ENV_PARAM, environment)
          																				 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
          																				 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
          																				 .replace(UobConstants.FILENM_PARAM, controlFileNm);
      srcInfo.setControlFileDir(controlFileDir);

      srcInfo.setControlFileName(controlFileNm);
      String controlFileFormat = StringUtils.trimToNull(ctrlfileSpec.get(UobConstants.FILE_FORMAT, String.class));
      FileLayout controlFileLayout = controlFileFormat == null ? VALID_FILE_LAYOUTS.get(srcFileFormat)
      		                                                     : VALID_FILE_LAYOUTS.get(controlFileFormat);
      if (controlFileLayout == null) {
      	throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Control file layout", "Valid file layouts are " + VALID_FILE_LAYOUTS.keySet());
      }
      
      srcInfo.setControlFileLayoutCd(controlFileLayout.fileFormat);
      srcInfo.setControlFileColumnDelimiter(controlFileLayout.fieldDelimiter);
      srcInfo.setControlFileTextDelimiter(controlFileLayout.stringEnclosure);
      
      // Decimal Indicator
      String ctrlDecimalInd = StringUtils.trimToEmpty(ctrlfileSpec.get(UobConstants.DEC_IND, String.class));
      if (!UobConstants.VALID_DEC_IND.contains(ctrlDecimalInd)) {
        throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, ctrlDecimalInd, "Invalid Decimal Indicator for file: " + controlFileNm);
      }
      
      srcInfo.setCtrlFileExplicitDecimalPoint(UobConstants.EXPLICIT.equalsIgnoreCase(ctrlDecimalInd) ? UobConstants.Y : UobConstants.N);
    } else {
      //check if it is Excel File Ingestion. If not set Control File Info to HT, otherwise set EmptyString
      // SM: Also included for SWIFT ingestion, as not control information is included.
      if(checkIfControlInfoRequired(srcInfo.getSourceFileLayoutCd())) {
    	  srcInfo.setControlInfo(UobConstants.CTRL_INFO_HT);
          srcInfo.setHeaderLines(Integer.parseInt(UobConstants.ONE));
          srcInfo.setTrailerLines(Integer.parseInt(UobConstants.ONE));
          srcInfo.setCtrlFileExplicitDecimalPoint(srcInfo.getExplicitDecimalPoint());
      }
    }
    
    srcInfo.setUserNm(UobConstants.SYS_USER_NAME);

    List<FieldModel> srcFieldInfo = new ArrayList<FieldModel>();
    String hashSumField = null;
    Map<String, String> uniqueMap = new HashMap<String, String>();
    int totalRecordLength = 0;
    int delimiterLength = StringUtils.trimToEmpty(srcInfo.getColumnDelimiter()).length();
    int fileReferenceFieldCount = 0;
    for (Entry<String, InterfaceSpecMap> keyEntry : srcFieldSpec.entrySet()) {
      InterfaceSpecMap fieldDef = keyEntry.getValue();
      
      // Field Name - Non Blank and Non Reserved Name
      String fieldName = fieldDef.get(UobConstants.FIELD_NM, String.class);
      if (StringUtils.isEmpty(fieldName)) {
        throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Field Name", "Field Name is Empty in Schema for file " + fileName);
      }
      
      // Data Type - Valid Data Types
      String dataType = fieldDef.get(UobConstants.FIELD_TYPE, String.class);
      boolean validDataType = UobConstants.VALID_SRC_DATATYPES.contains(dataType.trim());
      if (!validDataType) {
        throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, dataType, 
        		                              "Invalid Data Type in Schema for file: " + fileName + 
        		                              ", FieldName: " + fieldName);
      }
      // EDF-203
      if (UobConstants.SRC_FILE_REFERENCE.equalsIgnoreCase(dataType.trim())) {
      	fileReferenceFieldCount++;
      }

      // Length - Valid Lengths
      int length = fieldDef.get(UobConstants.FIELD_LEN, Integer.class);
      if (length <= 0) {
        throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, length, 
        		                              "Invalid Length in Schema for file: " + fileName + 
        		                   					  ", FieldName: " + fieldName);
      }

      // Valid Record Types
      RecordType recordType = fieldDef.get(UobConstants.RCRD_TYPE, RecordType.class);
      
      if (recordType.isHeader()) {
      	containsHeader = true;
      } 
      
      if (recordType.isFooter()) {
      	containsFooter = true;
      }
      
      // Format - Not NULL
      if (UobConstants.SRC_DATE.equalsIgnoreCase(dataType) || 
      		UobConstants.SRC_TIMESTAMP.equalsIgnoreCase(dataType)) {
        String dateFormat = fieldDef.get(UobConstants.FORMAT, String.class);
        if (StringUtils.isBlank(dateFormat)) {
        	String type = UobConstants.SRC_DATE.equalsIgnoreCase(dataType) ? "Date" : "Timestamp";
          throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, type + " format", 
          		                              type + " format is empty for file: " + fileName + 
          		                              ", FieldName: " + fieldName);
        }
      }

      int fieldNum = fieldDef.get(UobConstants.FIELD_NUM, Integer.class);
      
      // Create Field Model
      FieldModel fldModel = new FieldModel();
      fldModel.setFieldName(fieldName);
      fldModel.setFieldNum(fieldNum);
      fldModel.setOptionality(fieldDef.get(UobConstants.MDT_OPT, String.class));
      String fieldDesc = StringUtils.trimToEmpty(fieldDef.get(UobConstants.FIELD_DESC, String.class));
      fieldDesc = fieldDesc.replaceAll(UobConstants.QUOTE, UobConstants.EMPTY);
      fieldDesc = fieldDesc.replaceAll(UobConstants.SEMICOLON, UobConstants.COMMA);
      fldModel.setFieldDesc(fieldDesc);
      fldModel.setDataType(dataType);
      fldModel.setLength(length);
      totalRecordLength += length + delimiterLength;
      int decimalPrec = fieldDef.get(UobConstants.DEC_PREC, Integer.class);
      fldModel.setDecimalPrecision(decimalPrec);
      fldModel.setDataFormat(fieldDef.get(UobConstants.FORMAT, String.class));
      fldModel.setDefaultValue(fieldDef.get(UobConstants.DEFAULT_VAL, String.class));
      fldModel.setRecordType(recordType);
      fldModel.setExcelHeaderMappingField(fieldDef.get(UobConstants.EXCEL_FIELD_HEADER_MAPPING, String.class));
      // the extra 17 columns
      fldModel.setBizTerm(fieldDef.get(UobConstants.BIZ_TERM, String.class));
      fldModel.setBizDefinition(fieldDef.get(UobConstants.BIZ_DEFINITION, String.class));
      fldModel.setSynonyms(fieldDef.get(UobConstants.SYNONYMS, String.class));
      fldModel.setUsageContext(fieldDef.get(UobConstants.USAGE_CONTEXT, String.class));
      fldModel.setSystemSteward(fieldDef.get(UobConstants.SYSTEM_STEWARD, String.class));
      fldModel.setSourceSystem(fieldDef.get(UobConstants.SOURCE_SYSTEM, String.class));
      fldModel.setSourceTable(fieldDef.get(UobConstants.SOURCE_TABLE, String.class));
      fldModel.setSourceFieldName(fieldDef.get(UobConstants.SOURCE_FIELD_NAME, String.class));
      fldModel.setSourceFieldDesc(fieldDef.get(UobConstants.SOURCE_FIELD_DESC, String.class));
      fldModel.setSourceFieldType(fieldDef.get(UobConstants.SOURCE_FIELD_TYPE, String.class));
      fldModel.setSourceFieldLength(fieldDef.get(UobConstants.SOURCE_FIELD_LENGTH, Integer.class));
      fldModel.setSourceFieldFormat(fieldDef.get(UobConstants.SOURCE_FIELD_FORMAT, String.class));
      fldModel.setSourceDataCategory(fieldDef.get(UobConstants.SOURCE_DATA_CATEGORY, String.class));
      fldModel.setLOVCodeAndDesc(fieldDef.get(UobConstants.LOV_CODE_AND_DESC, String.class));
      fldModel.setOptionality2(fieldDef.get(UobConstants.OPTIONALITY_2, String.class));
      fldModel.setSysdataValidationLogic(fieldDef.get(UobConstants.SYSDATA_VALIDATION_LOGIC, String.class));
      fldModel.setDataAvailability(fieldDef.get(UobConstants.DATA_AVAILABILITY, String.class));
      fldModel.setFieldRegExpression(fieldDef.get(UobConstants.REGULAR_EXPRESSION, String.class));

      List<Integer> rulesList = new ArrayList<Integer>();

      // Implicit to Explicit Decimal Conversion
      if (UobConstants.IMPLICIT.equalsIgnoreCase(decimalInd) && 
      		(UobConstants.SRC_SIGNED_DECIMAL.equalsIgnoreCase(dataType) || 
      		 UobConstants.SRC_NUMERIC.equalsIgnoreCase(dataType) || 
      		 UobConstants.SRC_PACKED.equalsIgnoreCase(dataType))) {
        if (StringUtils.isEmpty(fldModel.getDataFormat())) {
          boolean set = false;
          if (decimalPrec != 0) {
            for (RuleModel model : stdList) {
              String ruleDesc = UobConstants.IMPLICIT_RULE_PATTERN + decimalPrec;
              if (model.getRuleDesc().trim().equalsIgnoreCase(ruleDesc)) {
                logger.debug("Adding Implicit Decimal Rule: " + model.getRuleId());
                rulesList.add(model.getRuleId());
                set = true;
              }
            }
            
            if (!set) {
              throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, 
              		                              "Implicit to Explicit Rule is missing", "Precision = " + decimalPrec);
            }
          }
        }
      }

      // Set Default Values
      if (StringUtils.isNotEmpty(fldModel.getDefaultValue())) {
        rulesList.add(Integer.parseInt(UobConstants.TWO));
      }
      
      // Remove Junk Characters for all Fields
      rulesList.add(Integer.parseInt(UobConstants.THREE));

      // Remove NULLS for all fields
      rulesList.add(Integer.parseInt(UobConstants.FOUR));

      // Date Formats
      if (StringUtils.isNotEmpty(fldModel.getDataFormat()) && 
      		!(recordType.isHeader() || recordType.isFooter())) {
        String ignoreDateFormats = PropertyLoader.getProperty(UobConstants.IGNORE_DATE_FORMATS);
        String[] ignoreDateFormatArray = ignoreDateFormats.split(UobConstants.COMMA);
        
        if (!Arrays.asList(ignoreDateFormatArray).contains(fldModel.getDataFormat())) {
          boolean set = false;
          for (RuleModel model : stdList) {
            String ruleDesc = UobConstants.DATE_RULE_PATTERN + fldModel.getDataFormat();
            if (model.getRuleDesc() != null && model.getRuleDesc().trim().equalsIgnoreCase(ruleDesc)) {
              rulesList.add(model.getRuleId());
              set = true;
            }
          }
          
          if (!set) {
            throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Date Format Rule", 
            		                              "Data format: " + fldModel.getDataFormat() + ", file: " + fileName);
          }
        }
      }

      // Numeric Data Type Validation
      if ((UobConstants.SRC_SIGNED_DECIMAL.equalsIgnoreCase(fldModel.getDataType()) || 
      		 UobConstants.SRC_NUMERIC.equalsIgnoreCase(fldModel.getDataType())) && 
      		StringUtils.isBlank(fldModel.getDataFormat())) {
        rulesList.add(Integer.parseInt(UobConstants.TWENTY_FIVE));
      }

      fldModel.setRulesList(rulesList);

      
      if (UobConstants.FIXED_FILE.equalsIgnoreCase(srcInfo.getSourceFileLayoutCd()) ||
      		UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(srcInfo.getSourceFileLayoutCd())) { 
        // Start and End Position
        int startPos = fieldDef.get(UobConstants.FIELD_START_POS, Integer.class);
        int endPos = fieldDef.get(UobConstants.FIELD_END_POS, Integer.class);

        fldModel.setStartPosition(startPos);
        fldModel.setEndPosition(endPos);
      }

      if (recordType.isFooter()) {
        String fieldDescr = StringUtils.trimToEmpty(fieldDef.get(UobConstants.FIELD_DESC, String.class));
        if (fldModel.isHashsumValueField() && StringUtils.containsIgnoreCase(fieldDescr, UobConstants.HASHSUM_FLD_PATTERN)) {
          hashSumField = StringUtils.trimToEmpty(fieldDescr.substring(StringUtils.indexOfIgnoreCase(fieldDescr, UobConstants.HASHSUM_FLD_PATTERN) + 
          		                                                        UobConstants.HASHSUM_FLD_PATTERN.length()));
        }
      }

      // TODO
      // fldModel.setIndexField(isIndexField);
      // fldModel.setProfileField(isProfileField);
      fldModel.setUserNm(UobConstants.SYS_USER_NAME);

      String uniqueStr = fieldName + UobConstants.UNDERSCORE + recordType;
      if (uniqueMap.containsKey(uniqueStr)) {
        throw new EDAGValidationException("Duplicate Field Name: " + fldModel.getFieldName() + " , Record Type: " + 
                                          fldModel.getRecordTypeInd() + " on file: " + fileName);
      } else {
        uniqueMap.put(uniqueStr, uniqueStr);
      }

      srcFieldInfo.add(fldModel);
    }
    
    // EDF-203
    String maxFileReferences = StringUtils.trimToNull(PropertyLoader.getProperty(MAX_FILE_REFERENCES));
    int maxFileReferenceFieldCount = maxFileReferences == null ? DEFAULT_MAX_FILE_REFERENCES : Integer.parseInt(maxFileReferences);
    if (maxFileReferenceFieldCount < fileReferenceFieldCount) {
    	throw new EDAGValidationException(EDAGValidationException.MAX_FILE_REFERENCES_EXCEEDED, fileName, fileReferenceFieldCount, maxFileReferenceFieldCount);
    }
    
    totalRecordLength -= delimiterLength;
    String maxRecordLengthProperty = StringUtils.trimToNull(PropertyLoader.getProperty(getClass().getName() + "." + MAX_RECORD_LENGTH));
    int maxRecordLength = maxRecordLengthProperty == null ? -1 : Integer.parseInt(maxRecordLengthProperty);
    if (maxRecordLength >= 0 && totalRecordLength > maxRecordLength) {
    	logger.warn("Total record length (" + totalRecordLength + ") is more than max record length allowed (" + maxRecordLength + ")");
    }

    if (StringUtils.isNotBlank(hashSumField)) {
    	boolean hashSumFieldSet = false;
    	
	    for (FieldModel fldModel : srcFieldInfo) {
	      String fldNm = fldModel.getFieldName();
	      if (fldNm.equalsIgnoreCase(hashSumField)) {
	        fldModel.setHashSumField(true);
	        hashSumFieldSet = true;
	      }
	    }
	    
	    if (!hashSumFieldSet) {
	    	throw new EDAGValidationException("Hash sum column (" + hashSumField + ") not found in the field list");
	    }
    }
    
    if (containsControl && containsHeader && containsFooter) {
      srcInfo.setControlInfo(UobConstants.CTRL_INFO_HT);
      srcInfo.setControlFileDir(null);
      srcInfo.setControlFileName(null);
      srcInfo.setHeaderLines(Integer.parseInt(UobConstants.ONE));
      srcInfo.setTrailerLines(Integer.parseInt(UobConstants.ONE));
    }

    // Validate Control Info
    if (!UobConstants.REG_EXPRESSION.equalsIgnoreCase(srcInfo.getSourceFileLayoutCd())) {

      if (!containsHeader && !containsFooter) {
        if (!containsControl && checkIfControlInfoRequired(srcInfo.getSourceFileLayoutCd())) {
          throw new EDAGValidationException("Control Information not available for file: " + fileName);
        }
      } else if (containsHeader && !containsFooter) {
        throw new EDAGValidationException("Header Available, But Footer is missing for file: " + fileName);
      } else if (!containsHeader && containsFooter) {
        throw new EDAGValidationException("Footer Available, But Header is missing for file: " + fileName);
      } else {
    	 // EDF-39
        validateCompulsoryHTFields(srcFieldInfo); 
      }
    }
    
    srcInfo.setSrcFieldInfo(srcFieldInfo);

    DestModel destInfo = new DestModel(DestModel.Type.HADOOP);

    String stagingDbNamePattern = PropertyLoader.getProperty(UobConstants.TIER_1_HIVE_DB_NM);
    String stagingDbName = stagingDbNamePattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
        																			 .replace(UobConstants.ENV_PARAM, environment)
        																			 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
        																			 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
        																			 .replace(UobConstants.FILENM_PARAM, fileName);
    destInfo.setStagingDbName(stagingDbName);

    /*
     * HiveDAO dao = new HiveDAO(); boolean stgSchemaExists =
     * dao.checkSchemaExists(stagingDBName); if (stgSchemaExists) { throw new
     * UOBException("Hive Schema:" + stagingDBName + " already exists",
     * UOBConstants.VALIDATION_FAILURE); }
     */

    String stagingDirPattern = PropertyLoader.getProperty(UobConstants.TIER_1_HDFS_PATH);
    String stagingDir = stagingDirPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
        																 .replace(UobConstants.ENV_PARAM, environment)
        																 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
        																 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
        																 .replace(UobConstants.FILENM_PARAM, fileName);
    destInfo.setStagingDir(stagingDir);
    
    String stagingHivePartitionPattern = PropertyLoader.getProperty(UobConstants.TIER_1_HIVE_PARTITION);
    String t1PartitionInfo = procSpec.get(UobConstants.T1_PARTITION, String.class);
    StringBuilder stagingHivePartition = new StringBuilder();
    if (StringUtils.isNotBlank(t1PartitionInfo)) {
      String[] t1PartitionArr = t1PartitionInfo.split(UobConstants.COMMA, -1);
      for (String partCol : t1PartitionArr) {
        boolean validPartition = UobConstants.VALID_PARTITION_PARAMS.contains(partCol.trim());
        if (!validPartition) {
          throw new EDAGValidationException("Invalid Partition Column: " + partCol + " for file: " + fileName);
        }
        
        if (!"".equalsIgnoreCase(stagingHivePartition.toString())) {
          stagingHivePartition.append(UobConstants.COMMA);
        }
        
        if (UobConstants.SITE_ID.equalsIgnoreCase(partCol)) {
          partCol = "ctry_cd";
        }
        
        stagingHivePartition.append(partCol + "=<" + partCol + ">");
      }

      String newStagingHivePartition = stagingHivePartition.toString().replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd());
      destInfo.setStagingHivePartition(newStagingHivePartition);
    } else {
      String newStagingHivePartition = stagingHivePartitionPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
          																												.replace(UobConstants.ENV_PARAM, environment)
          																												.replace(UobConstants.ENV_NUM_PARAM, environmentNum)
          																												.replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
          																												.replace(UobConstants.FILENM_PARAM, fileName);
      destInfo.setStagingHivePartition(newStagingHivePartition);
    }

    String stagingHiveLocationPattern = PropertyLoader.getProperty(UobConstants.TIER_1_HIVE_LOCATION);
    String stagingHiveLocation = stagingHiveLocationPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
        																									 .replace(UobConstants.ENV_PARAM, environment)
        																									 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
        																									 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
        																									 .replace(UobConstants.FILENM_PARAM, fileName);
    destInfo.setStagingHiveLocation(stagingHiveLocation);
    /*
    String dumpStagingHiveLocationPattern = PropertyLoader.getProperty(UobConstants.TIER_1_HIVE_DUMP_LOCATION);
    String dumpStagingHiveLocation = dumpStagingHiveLocationPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
				 																													 .replace(UobConstants.ENV_PARAM, environment)
				 																													 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
				 																													 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
				 																													 .replace(UobConstants.FILENM_PARAM, fileName);
    destInfo.setDumpStagingHiveLocation(dumpStagingHiveLocation);
*/
    String stagingTableNamePattern = PropertyLoader.getProperty(UobConstants.TIER_1_HIVE_TBL_NM);
    String stagingTableName = stagingTableNamePattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
        																						 .replace(UobConstants.ENV_PARAM, environment)
        																						 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
        																						 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
        																						 .replace(UobConstants.FILENM_PARAM, fileName);
    destInfo.setStagingTableName(stagingTableName);
    
    String hiveDbNamePattern = PropertyLoader.getProperty(UobConstants.DDS_HIVE_DB_NM);
    String hiveDbName = hiveDbNamePattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
        																 .replace(UobConstants.ENV_PARAM, environment)
        																 .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
        																 .replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
        																 .replace(UobConstants.FILENM_PARAM, fileName);
    destInfo.setHiveDbName(hiveDbName);
    destInfo.setLoadTypeCd(setLoadType(loadStrategy));

    String hivePartitionPattern = PropertyLoader.getProperty(UobConstants.DDS_HIVE_PARTITION);
    if (UobConstants.HISTORY_LOAD_CD.equalsIgnoreCase(destInfo.getLoadTypeCd())) {
    	hivePartitionPattern = PropertyLoader.getProperty(UobConstants.DDS_HIVE_PARTITION_WITH_HISTORY);
    }
    String t11PartitionInfo = procSpec.get(UobConstants.T11_PARTITION, String.class);
    StringBuilder hivePartition = new StringBuilder();
    if (StringUtils.isNotBlank(t11PartitionInfo)) {
      String[] t11PartitionArr = t11PartitionInfo.split(UobConstants.COMMA, -1);
      for (String partCol : t11PartitionArr) {
        boolean validPartition = UobConstants.VALID_PARTITION_PARAMS.contains(partCol.trim());
        if (!validPartition) {
          throw new EDAGValidationException("Invalid Partition Column: " + partCol + " for file: " + fileName);
        }
        
        if (!"".equalsIgnoreCase(hivePartition.toString())) {
          hivePartition.append(UobConstants.COMMA);
        }
        
        hivePartition.append(partCol.trim() + "=<" + partCol.trim() + ">");
      }

      String newHivePartition = hivePartition.toString().replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd());
      destInfo.setHivePartition(newHivePartition);
    } else {
      String newHivePartition = hivePartitionPattern.replace(UobConstants.SRC_SYS_PARAM, procModel.getSrcSysCd())
          																					.replace(UobConstants.ENV_PARAM, environment)
          																					.replace(UobConstants.ENV_NUM_PARAM, environmentNum)
          																					.replace(UobConstants.FREQ_PARAM, procModel.getProcFreqCd())
          																					.replace(UobConstants.FILENM_PARAM, fileName);
      destInfo.setHivePartition(newHivePartition);
    }

    String hiveTableName = (String) procSpec.get(UobConstants.TABLE_NAME);
    destInfo.setHiveTableName(hiveTableName);
    double hiveErrorThreshold = (double) procSpec.get(UobConstants.ERROR_THRESHOLD);
    destInfo.setHiveErrorThreshold(hiveErrorThreshold);
    destInfo.setHadoopCompressCd(UobConstants.SNAPPY_CD);
    destInfo.setHadoopFormatCd(UobConstants.PARQUET_CD);
    Map<Integer, FieldModel> destFieldInfo = new HashMap<Integer, FieldModel>();
    if (srcInfo.getSrcFieldInfo() != null && !srcInfo.getSrcFieldInfo().isEmpty()) {
      String ignoreDateFormats = PropertyLoader.getProperty(UobConstants.IGNORE_DATE_FORMATS);
      String[] ignoreDateFormatArray = ignoreDateFormats.split(UobConstants.COMMA);

      // EDF-203
      // List<FieldModel> refFileFields = new ArrayList<FieldModel>();
      
      // int maxFieldNum = -1;
      for (FieldModel destModel : srcInfo.getSrcFieldInfo()) {
        // Assuming all fields without empty data formats are always dates
        if (StringUtils.isNotEmpty(destModel.getDataFormat())) {
          if (!Arrays.asList(ignoreDateFormatArray).contains(destModel.getDataFormat())) {  
            destModel.setDataType(UobConstants.SRC_TIMESTAMP);
          }
        }
        
        if (!destModel.getRecordType().isHeader() && !destModel.getRecordType().isFooter()) {
          destFieldInfo.put(destModel.getFieldNum(), destModel);
          
          // EDF-203
          /*
          maxFieldNum = maxFieldNum < destModel.getFieldNum() ? destModel.getFieldNum() : maxFieldNum;
          if (UobConstants.SRC_FILE_REFERENCE.equals(destModel.getDataType())) {
          	refFileFields.add(destModel);
          }
          */
        }
      }
      
      String referencedFieldSuffix = procModel.getProcParam(UobConstants.PROC_PARAM_REFERENCED_FILE_FIELDNAME_SUFFIX);
      if (StringUtils.isBlank(referencedFieldSuffix)) {
      	referencedFieldSuffix = FieldModel.DEFAULT_REFERENCED_FILE_FIELD_NAME_SUFFIX;
      }
      
      // EDF-203
      /*
      for (FieldModel refFileField : refFileFields) {
      	FieldModel refFileLocationField = new FieldModel();
      	refFileLocationField.setFieldNum(++maxFieldNum);
      	refFileLocationField.setRecordType(RecordType.FIELD_INFO);
      	refFileLocationField.setFieldName(refFileField.getFieldName() + referencedFieldSuffix);
      	refFileLocationField.setFieldDesc("Location of file referenced by field " + refFileField.getFieldName());
      	refFileLocationField.setDataType(UobConstants.SRC_T11_FILE_REFERENCE);
      	destFieldInfo.put(maxFieldNum, refFileLocationField);
      }
      */
    }

    destInfo.setDestFieldInfo(destFieldInfo);
    destInfo.setUserNm(UobConstants.SYS_USER_NAME);

    procModel.setDestInfo(destInfo);

    // Alert Email
    String email = procSpec.get(UobConstants.ALERT_EMAIL, String.class);
    if (StringUtils.isBlank(email)) {
      logger.warn("Alert Email is empty for file: " + fileName + ", No alerts will be configured");
    } else {
      AlertModel alertInfo = new AlertModel();
      int alertId = regisDao.selectAlertId();
      alertInfo.setAlertId(alertId);
      alertInfo.setEmail(email);
      alertInfo.setProcId(processId);
      procModel.setAlertInfo(alertInfo);
    }

    if (containsControl && !containsHeader && !containsFooter) {
      Map<String, InterfaceSpecMap> ctrlFieldSpec = spec.getCtrlFieldSpec();
      for (Entry<String, InterfaceSpecMap> keyEntry : ctrlFieldSpec.entrySet()) {
        InterfaceSpecMap fieldDef = keyEntry.getValue();
  
        // Field Name - Non Blank and Non Reserved Name
        String fieldName = fieldDef.get(UobConstants.FIELD_NM, String.class);
        if (StringUtils.isEmpty(fieldName)) {
          throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Field Name", "File: " + fileName);
        }
  
        // Data Type - Valid Data Types
        String dataType = fieldDef.get(UobConstants.FIELD_TYPE, String.class);
        boolean validDataType = UobConstants.VALID_SRC_DATATYPES.contains(dataType.trim());
        if (!validDataType) {
          throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, dataType, 
          		                              "Invalid Data Type in Schema for file: " + fileName + 
          		                    					", FieldName: " + fieldName);
        }
  
        // Length - Valid Lengths
        int length = fieldDef.get(UobConstants.FIELD_LEN, Integer.class);
        if (length <= 0) {
          throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, length, 
          		                              "Invalid Length in Schema for file: " + fileName + 
          		                              ", FieldName: " + fieldName);
        }
  
        // Valid Record Types
        RecordType recordType = fieldDef.get(UobConstants.RCRD_TYPE, RecordType.class);
        
        // Format - Not NULL
        if (UobConstants.SRC_DATE.equalsIgnoreCase(dataType)) {
          String dateFormat = fieldDef.get(UobConstants.FORMAT, String.class);
          if (StringUtils.isBlank(dateFormat)) {
            throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Date Format",
            		                              "File: " + fileName + ", FieldName: " + fieldName);
          }
        }
  
        int fieldNum = fieldDef.get(UobConstants.FIELD_NUM, Integer.class);
        
        // Create Field Model
        FieldModel fldModel = new FieldModel();
        fldModel.setFieldName(fieldName);
        fldModel.setFieldNum(fieldNum);
        fldModel.setOptionality(fieldDef.get(UobConstants.MDT_OPT, String.class));
        fldModel.setFieldDesc(fieldDef.get(UobConstants.FIELD_DESC, String.class));
        fldModel.setDataType(dataType);
        fldModel.setLength(length);
        int decimalPrec = fieldDef.get(UobConstants.DEC_PREC, Integer.class);
        fldModel.setDecimalPrecision(decimalPrec);
        fldModel.setDataFormat(fieldDef.get(UobConstants.FORMAT, String.class));
        fldModel.setDefaultValue(fieldDef.get(UobConstants.DEFAULT_VAL, String.class));
        fldModel.setRecordType(recordType);

        boolean fallback = false;
        String layoutCode = StringUtils.trimToNull(srcInfo.getControlFileLayoutCd());
        if (layoutCode == null) {
        	fallback = true;
        	layoutCode = srcInfo.getSourceFileLayoutCd();
        }
        
        if (UobConstants.FIXED_FILE.equalsIgnoreCase(layoutCode) ||
        		UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(layoutCode) || 
            (fallback && UobConstants.BELL_CHAR.equals(srcInfo.getColumnDelimiter()))) {
          // Start and End Position
          int startPos = fieldDef.get(UobConstants.FIELD_START_POS, Integer.class);
          int endPos = fieldDef.get(UobConstants.FIELD_END_POS, Integer.class);
  
          fldModel.setStartPosition(startPos);
          fldModel.setEndPosition(endPos);
        }
  
        if (recordType.isFooter()) {
          String fieldDesc = StringUtils.trimToEmpty(fieldDef.get(UobConstants.FIELD_DESC, String.class));
          if (fldModel.isHashsumValueField() && StringUtils.containsIgnoreCase(fieldDesc, UobConstants.HASHSUM_FLD_PATTERN)) {
            hashSumField = StringUtils.trimToEmpty(fieldDesc.substring(StringUtils.indexOfIgnoreCase(fieldDesc, UobConstants.HASHSUM_FLD_PATTERN) + 
            		                                                       UobConstants.HASHSUM_FLD_PATTERN.length()));
          }
        }
        
        fldModel.setUserNm(UobConstants.SYS_USER_NAME);
  
        String uniqueStr = fieldName + UobConstants.UNDERSCORE + recordType;
        if (uniqueMap.containsKey(uniqueStr)) {
          throw new EDAGValidationException("Duplicate Field Name: " + fldModel.getFieldName() + " , Record Type: " + 
                                            fldModel.getRecordTypeInd() + " on file: " + fileName);
        } else {
          uniqueMap.put(uniqueStr, uniqueStr);
        }
    
        ctrlFieldInfo.add(fldModel);
        srcInfo.setCtrlInfo(ctrlFieldInfo);
      }
      
      if(!spec.getSrcSystem().equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_SRC_SYS_CD)) {
    	  // EDF-39
    	  validateCompulsoryControlFields(ctrlFieldInfo);
      }
    }
    
    procModel.setSrcInfo(srcInfo);
    logger.info("Interface Spec Validation and Conversion completed for process: " + processId);

    return procModel;
  }

  private boolean checkIfControlInfoRequired(String fileLayoutCD) {
	return !(UobConstants.XLS_FILE_WITH_HEADER.equals(fileLayoutCD) 
  		  || UobConstants.XLS_FILE_WITHOUT_HEADER.equals(fileLayoutCD) 
  		  || UobConstants.XLSX_FILE_WITH_HEADER.equals(fileLayoutCD) 
  		  || UobConstants.XLSX_FILE_WITHOUT_HEADER.equals(fileLayoutCD)
        || UobConstants.CSV.equals(fileLayoutCD) // for Swift File, EDF 225, no Footer control info or separate control file.
  		  || UobConstants.REG_EXPRESSION.equals(fileLayoutCD));
  }

  private void validateCompulsoryHTFields(List<FieldModel> srcFieldInfo) throws EDAGValidationException {
		boolean hasBusinessDate = false;
		boolean hasRecordCount = false;
		boolean hasHeaderIndicator = false;
		boolean hasFooterIndicator = false;
		
		for (FieldModel fieldModel : srcFieldInfo) {
			if (!hasBusinessDate && fieldModel.getRecordType().isHeader() && fieldModel.isBusinessDateField()) {
				hasBusinessDate = true;
			} else if (!hasHeaderIndicator && fieldModel.getRecordType().isHeader() && fieldModel.isRecordTypeIndicatorField()) {
				hasHeaderIndicator = true;
			} else if (!hasRecordCount && (fieldModel.getRecordType().isFooter() && fieldModel.isRecordCountField() || 
					fieldModel.getRecordType().isHeader() && fieldModel.isRecordCountField())) {
				hasRecordCount = true;
			} else if (!hasFooterIndicator && fieldModel.getRecordType().isFooter() && fieldModel.isRecordTypeIndicatorField()) {
				hasFooterIndicator = true;
			}
		}
		
		if (!hasBusinessDate) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Business date", "Business date field is recognized by 'EOD BUSINESS DT' or 'Business date', or 'Cycle Date' in the field name or field description (case insensitive)");
		}  
		
		if (!hasRecordCount) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Record count", "Record count field is recognized by 'TOT NO OF RECORDS' or 'Record count' or 'Count' in the field name or field description (case insensitive)");
		}
		
		if (!hasHeaderIndicator) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Header indicator", "Header indicator is recognized by 'Record Type' in the field name (case insensitive)");
		}
		
		if (!hasFooterIndicator) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Footer indicator", "Footer indicator is recognized by 'Record Type' in the field name (case insensitive)");
		}
	}

	private void validateCompulsoryControlFields(List<FieldModel> ctrlFieldInfo) throws EDAGValidationException {
  	boolean hasBaseFile = false;
  	boolean hasBusinessDate = false;
  	boolean hasRecordCount = false;
  	
		for (FieldModel fieldModel : ctrlFieldInfo) {
			if (!hasBusinessDate && fieldModel.isBusinessDateField()) {
				hasBusinessDate = true;
			} else if (!hasBaseFile && fieldModel.isFileNameField()) {
				hasBaseFile = true;
			} else if (!hasRecordCount && fieldModel.isRecordCountField()) {
				hasRecordCount = true;
			}
		}
		
		if (!hasBusinessDate) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Business date", "Business date field is recognized by 'EOD BUSINESS DT' or 'Business date', or 'Cycle Date' in the field name or field description (case insensitive)");
		}  
		
		if (!hasBaseFile) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Base file", "Base file field is recognized by 'BASE SYS FILE' or 'Base File' in the field name or field description (case insensitive)");
		}
		
		if (!hasRecordCount) {
			throw new EDAGValidationException(EDAGValidationException.MISSING_VALUE, "Record count", "Record count field is recognized by 'TOT NO OF RECORDS' or 'Record count' or 'Count' in the field name or field description (case insensitive)");
		}
	}

	/**
   * This method is used to set the Load Type Code from the Load Type Description.
   * @param loadType The Load Type Description
   * @return the Load Type Code
   */
  private String setLoadType(String loadType) {
  	return LOAD_TYPE_MAP.get(StringUtils.trimToEmpty(loadType).toUpperCase());
  }

  /**
   * This method is used to set the File Type code from the File Type Description.
   * @param fileType The File Type Description
   * @return the File Type Code
   */
  private String setFileType(String fileType) {
    String fileTypeCd = null;
    
    if (UobConstants.MASTER.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.MASTER_FILE;
    }
    
    if (UobConstants.PARAMETER.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.PARAMETER_FILE;
    }
    
    if (UobConstants.TRANSACTION.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.TRANSACTION_FILE;
    }
    
    if (UobConstants.ENRICHED.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.ENRICHED_FILE;
    }
    
    if (UobConstants.TECHRECON.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.TECHRECON_FILE;
    }
    
    if (UobConstants.LOG.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.LOG_FILE;
    }
    
    if (UobConstants.INTERFACE.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.INTERFACE_FILE;
    }
    
    if (UobConstants.CTRL_FILE.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.CONTROL_FILE;
    }
    
    if (UobConstants.MAPPING.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.MAPPING_FILE;
    }
    
    if (UobConstants.REFERENCE.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.REFERENCE_FILE;
    }
    
    if (UobConstants.WORK.equalsIgnoreCase(fileType.trim())) {
      fileTypeCd = UobConstants.WORK_FILE;
    }
    
    return fileTypeCd;
  }

  /**
   * This method is used to set the File Layout code from the File Layout Description.
   * @param fileLayout The File Layout Description
   * @param srcInfo The File Model object into which the file layout will be set
   * @return the File Model object with the File Layouts set
   */
  protected FileModel setFileLayout(String fileLayout, FileModel srcInfo) throws EDAGValidationException {
  	FileLayout layout = VALID_FILE_LAYOUTS.get(StringUtils.trimToEmpty(fileLayout));
  	if (layout == null) {
  		throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "File layout", fileLayout + " is not a valid file layout. Valid file layouts are " + VALID_FILE_LAYOUTS.keySet());
  	}
  	
  	srcInfo.setSourceFileLayoutCd(layout.fileFormat);
  	srcInfo.setTextDelimiter(layout.stringEnclosure);
  	srcInfo.setColumnDelimiter(layout.fieldDelimiter);
    
    return srcInfo;
  }

  /**
   * This method is used to add a country to the list of applicable countries
   *     if it is applicable for the given process.
   * @param countryCol The column holding the value of the Country
   * @param ctryCd The Country Code
   * @param countryCdList The existing Country Code List
   * @param srcFileInfo The parsed source file specification
   * @return the List of Country Codes which are applicable
   * @throws EDAGValidationException 
   */
  private void addCountryIfApplicable(String countryCode, ProcessModel processModel, 
  		                                InterfaceSpecMap srcFileInfo) throws EDAGValidationException {
  	Boolean supported = srcFileInfo.get(UobConstants.getCountryInterfaceSpecKey(countryCode), Boolean.class);
  	if (supported == null) {
  		supported = false;
  	}
  	
  	if (supported) {
  		CountryAttributes attrs = processModel.addCountry(countryCode);
  		attrs.setCharset(srcFileInfo.get(UobConstants.getCharsetInterfaceSpecKey(countryCode), String.class));
  		attrs.setEncoding(srcFileInfo.get(UobConstants.getEncodingInterfaceSpecKey(countryCode), String.class));
  		attrs.setReferencedFileFolder(srcFileInfo.get(UobConstants.getReferencedFileFolderSpecKey(countryCode), String.class));
  	}
  }
  
  /**
   * This method is used to transform the Export Process Specification into a list of
   *     Process Model objects.
   * @param spec The Process Spec object
   * @param force Force flag
   * @param filesToForce List of files to be force processed
   * @param filesToProcess List of files to be processed
   * @return a List of Process Model objects
   * @throws EDAGMyBatisException 
   * @throws Exception when there is an error transforming the Interface Specification
   *     into the Process Model.
   */
  public ProcessModel handleExportSpecDetails(InterfaceSpec spec, boolean force, String filesToForce, 
  		                                        String filesToProcess, String processId, 
  		                                        InterfaceSpecMap procSpec) throws EDAGValidationException, EDAGMyBatisException {

    List<String> forceFilesList = null;
    boolean forceAll = false;
    if (force && filesToForce != null && "*".equalsIgnoreCase(filesToForce)) {
      forceAll = true;
    } else if (force && filesToForce != null && !"".equalsIgnoreCase(filesToForce)) {
      String[] forceFilesArr = filesToForce.split(UobConstants.COMMA);
      forceFilesList = Arrays.asList(forceFilesArr);
      logger.debug("Force Files List is:" + Arrays.toString(forceFilesList.toArray()));
    }

    RegistrationDao regisDao = new RegistrationDao();

    logger.debug("Going to validate spec for Process ID: " + processId);

    /// File Name - Non Blank
    if (StringUtils.isEmpty(processId)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process ID", "Process ID is empty in Process Spec");
    }

    if (StringUtils.isNotEmpty(filesToProcess) && !"*".equalsIgnoreCase(filesToProcess)) {
      String[] filesToProcessArr = filesToProcess.split(UobConstants.COMMA);
      boolean process = false;
      for (String fileProc : filesToProcessArr) {
      	process = fileProc != null && fileProc.trim().equalsIgnoreCase(processId);
      }
      
      if (!process) {
        return null;
      }
    }
    
    // File Name - Unique
    if (!forceAll) {
      if (!force || forceFilesList == null) {
        if (regisDao.checkFileExists(processId)) {
          throw new EDAGValidationException(EDAGValidationException.PROCESS_ALREADY_EXISTS, processId);
        }
      } else {
        if (forceFilesList.size() == 0) {
          if (regisDao.checkFileExists(processId)) {
            throw new EDAGValidationException(EDAGValidationException.PROCESS_ALREADY_EXISTS, processId);
          }
        } else if (forceFilesList.size() > 0 && !forceFilesList.contains(processId)) {
          if (regisDao.checkFileExists(processId)) {
            throw new EDAGValidationException(EDAGValidationException.PROCESS_ALREADY_EXISTS, processId);
          }
        } else {
          logger.debug("Not validating process existing check for: " + processId + ", force: " + force);
        }
      }
    }
      
    // Source System Validation - Process Spec
    String procSrcSysName = procSpec.get(UobConstants.SRC_SYS_NM, String.class);
    if (StringUtils.isBlank(procSrcSysName)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Source System Name", "Process source system name is empty");
    }
    
    // Process Name Validation
    String processNm = procSpec.get(UobConstants.PROCESS_NAME, String.class);
    if (StringUtils.isBlank(processNm)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Name", "Process Name is Empty");
    }
  
    // Process Group Validation
    String processGrpNm = procSpec.get(UobConstants.PROCESS_GRP, String.class);
    if (StringUtils.isBlank(processGrpNm)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Process Group", "Process Group is Empty");
    }
  
    // Country Code Validation
    String countryCd = procSpec.get(UobConstants.CTRY_CD, String.class);
    if (StringUtils.isBlank(countryCd)) {
      logger.warn("Country Code is Empty for Process: " + processNm);
    }
    
    // Source Database Name Validation
    String srcDbName = procSpec.get(UobConstants.SRC_DB_NAME, String.class);
    if (StringUtils.isBlank(srcDbName)) {
      logger.warn("Source Database Name is Empty for Process: " + processNm);
    }
    
    // Source Table Name Validation
    String srcTblName = procSpec.get(UobConstants.SRC_TBL_NAME, String.class);
    if (StringUtils.isBlank(srcTblName)) {
      logger.warn("Source Table Name is Empty for Process: " + processNm);
    }
    
    // Target Directory Validation
    String tgtDir = procSpec.get(UobConstants.TGT_DIR_NAME, String.class);
    if (StringUtils.isBlank(tgtDir)) {
      logger.warn("Target Directory is Empty for Process: " + processNm);
    }
    
    // Target File Name Validation
    String tgtFile = procSpec.get(UobConstants.TGT_FILE_NAME, String.class);
    if (StringUtils.isBlank(tgtFile)) {
      logger.warn("Target File Name is Empty for Process: " + processNm);
    }
    
    // Column Delimiter Validation
    String colDelim = procSpec.get(UobConstants.TGT_COL_DELIM, String.class);
    if (StringUtils.isBlank(colDelim)) {
      logger.warn("Column Delimiter is Empty for Process: " + processNm);
    }
    
    // Control File Name Validation
    String ctrlFile = procSpec.get(UobConstants.CTRL_FILE_NM, String.class);
    if (StringUtils.isBlank(ctrlFile)) {
      logger.info("Control File is Empty for Process: " + processNm);
    }
    
    // Deployment Edge Node Validation
    String deployNode = procSpec.get(UobConstants.DEPLOYMENT_NODE, String.class);
    if (StringUtils.isBlank(deployNode)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Deployment Node", "Deployment Node is Empty");
    }
    
    ProcessModel procModel = new ProcessModel();

    procModel.setProcId(processId);
    procModel.setProcNm(processId);
    procModel.setProcTypeCd(UobConstants.TPT_EXPORT_PROC_TYPE);
    
    // Process Group Validation
    String procGrpId = procSpec.get(UobConstants.PROCESS_GRP, String.class);
    if (StringUtils.trimToNull(procGrpId) != null) {
    	procModel.setProcGrpId(Double.valueOf(procGrpId).intValue());
    }

    // Frequency
    procModel.setProcFreqCd(procSpec.get(UobConstants.FREQ_CD, String.class));

    procModel.setSrcSysCd(procSrcSysName);
    procModel.setUserNm(UobConstants.SYS_USER_NAME);
    procModel.setDeployNodeNm(deployNode);
    procModel.addCountry(countryCd);

    List<ProcessParam> paramList = new ArrayList<ProcessParam>();
    
    // Params
    Map<String, Map<String, String>> paramSpec = spec.getParamSpec();
    Map<String, String> downstreamSpec = spec.getDownstreamSpec();
    if (paramSpec != null && paramSpec.containsKey(processId)) {
      Map<String, String> procParamList = paramSpec.get(processId);
      if (procParamList != null) {
        for (Entry<String, String> entry : procParamList.entrySet()) {
          ProcessParam param = new ProcessParam();
          param.setParamName(entry.getKey());
          param.setParamValue(entry.getValue());
          paramList.add(param);
        }
      }
    }

    procModel.setProcParam(paramList);
    
    // Set Downstream Application
    if (downstreamSpec != null && downstreamSpec.containsKey(processId)) {
      String downstreamAppName = downstreamSpec.get(processId);
      if (StringUtils.isNotBlank(downstreamAppName)) {
        procModel.setDownstreamAppl(downstreamAppName);
      }
    }

    DestModel destInfo = new DestModel();
    destInfo.setProcessId(processId);
    destInfo.setSrcDbName(srcDbName);
    destInfo.setSrcTblName(srcTblName);
    destInfo.setTgtDirName(tgtDir);
    destInfo.setTgtFileName(tgtFile);
    destInfo.setTgtFileExtn(procSpec.get(UobConstants.TGT_FILE_EXTN, String.class));
    destInfo.setTgtColDelim(colDelim);
    destInfo.setTgtTxtDelim(procSpec.get(UobConstants.TGT_TXT_DELIM, String.class));
    destInfo.setCtrlFileName(ctrlFile);
    destInfo.setCtrlFileExtn(procSpec.get(UobConstants.CTRL_FILE_EXTN, String.class));
    
    destInfo.setUserNm(UobConstants.SYS_USER_NAME);

    procModel.setDestInfo(destInfo);

    // Alert Email
    String email = procSpec.get(UobConstants.ALERT_EMAIL, String.class);
    if (StringUtils.isBlank(email)) {
      logger.warn("Alert Email is empty for process: " + processId + ", No alerts will be configured");
    } else {
      AlertModel alertInfo = new AlertModel();
      int alertId = regisDao.selectAlertId();
      alertInfo.setAlertId(alertId);
      alertInfo.setEmail(email);
      alertInfo.setProcId(processId);
      procModel.setAlertInfo(alertInfo);
    }
    
    logger.info("Process Spec Validation and Conversion completed for process: " + processId);

    return procModel;
  }
}
