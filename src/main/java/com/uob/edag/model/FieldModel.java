package com.uob.edag.model;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description : The file defines the Field Definition format. This file will
 *              be consumed in the Data Ingestion process. This file contains
 *              all the properties which will help in consuming the input file's
 *              schema details.
 * 
 */

public class FieldModel implements Cloneable, Comparable<FieldModel> {
	
	public static final String DEFAULT_REFERENCED_FILE_FIELD_NAME_SUFFIX = PropertyLoader.getProperty("ReferencedFile.URIField.Suffix") == null ? "_path" : PropertyLoader.getProperty("ReferencedFile.URIField.Suffix");
	
	private static final String HEADER = "HEADER";
	private static final String FOOTER = "FOOTER";
	
	public enum RecordType {
		PRIMARY_KEY("PK"), 
		FIELD_INFO("FI"), 
		FOREIGN_KEY("FK"), 
		BUSINESS_KEY("BK"),
		HEADER("HR"),
		FOOTER("FR"),
		CONTROL_INFO(UobConstants.CTRL_INFO_C);
		
		private final String type;
		
		RecordType(String type) {
			this.type = type;
		}
		
		public String toString() {
			return type;
		}
		
		public int getRanking() {
			return isHeader() ? 1 : isData() ? 2 : isFooter() ? 3 : 4;
		}
		
		public boolean isData() {
			return this == RecordType.PRIMARY_KEY || this == RecordType.FIELD_INFO || this == RecordType.FOREIGN_KEY ||
					   this == RecordType.BUSINESS_KEY;        
		}
		
		public boolean isHeader() {
			return this == RecordType.HEADER;
		}
		
		public boolean isFooter() {
			return this == RecordType.FOOTER;
		}
		
		public String getDefaultIndicator() {
			if (isHeader()) {
				return "H";
			} else if (isFooter()) {
				return "T";
			} else {
				return "D";
			}
		}
		
		public static RecordType fromString(String str) throws EDAGValidationException {
			String recTypeInd = StringUtils.trimToEmpty(str);
	  	
			if (StringUtils.isEmpty(recTypeInd)) {
				return RecordType.FIELD_INFO;
			} else if (FieldModel.HEADER.equalsIgnoreCase(recTypeInd)) {
	  		return HEADER;
	  	} else if (FieldModel.FOOTER.equalsIgnoreCase(recTypeInd)) {
	  		return FOOTER;
	  	} else {
		    for (RecordType recType : RecordType.values()) {
		    	if (recType.toString().equalsIgnoreCase(recTypeInd)) {
		    		return recType;
		    	}
		    }
	  	}
			
			throw new EDAGValidationException(EDAGValidationException.INVALID_EXPECTED_VALUE, "record type", 
					                 																															PRIMARY_KEY + " or " +
			                                                                                  FIELD_INFO + " or " +
			                                                                                  FOREIGN_KEY + " or " + 
			                                                                                  BUSINESS_KEY + " or " + 
			                                                                                  HEADER + " or " + 
			                                                                                  FOOTER + " or " + 
			                                                                                  CONTROL_INFO, str);
		}
	};
	
	private static final MessageFormat INSERT_FIELD_STD_RULES_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_FIELD_STD_RULES(FILE_ID, FLD_NM, RCRD_TYP_CD, RULE_ID, CRT_DT, CRT_USR_NM) ") +
      UobUtils.ltrim("VALUES({0, number, #}, ''{1}'', ''{2}'', {3, number, #}, DEFAULT, ''{4}''); ")
	);
	
	private static final MessageFormat INSERT_FIELD_DETAIL_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_FIELD_DETAIL(FILE_ID, RCRD_TYP_CD, FLD_NM, FLD_DESC, FLD_NUM, FLD_LEN_NUM, ") +
      UobUtils.ltrim("                              FLD_DEC_PREC, FLD_DATA_TYPE_TXT, FLD_FORMAT_TXT, FLD_DEF_VAL, ") +
      UobUtils.ltrim("                              FLD_START_POS_NUM, FLD_END_POS_NUM, IS_FLD_HASHSUM_FLG, ") +
      UobUtils.ltrim("                              IS_FLD_INDEX_FLG, IS_FLD_PROFILE_FLG, CRT_DT, CRT_USR_NM, FLD_OPTIONALITY, ") +
      UobUtils.ltrim(" 		                          FLD_BIZ_TERM, FLD_BIZ_DEFINITION, FLD_SYNONYMS, FLD_USAGE_CONTEXT, ") +
      UobUtils.ltrim("                              FLD_SYSTEM_STEWARD, FLD_SOURCE_SYSTEM, FLD_SOURCE_TABLE, FLD_SOURCE_FIELD_NAME, ") +
      UobUtils.ltrim("                              FLD_SOURCE_FIELD_DESC, FLD_SOURCE_FIELD_TYPE, FLD_SOURCE_FIELD_LENGTH, FLD_SOURCE_FIELD_FORMAT, ") +
      UobUtils.ltrim("                              FLD_SOURCE_DATA_CATEGORY, FLD_LOV_CODE_AND_DESC, FLD_OPTIONALITY_2, FLD_SYSDATA_VALIDATION_LOGIC, ") +
      UobUtils.ltrim("                              FLD_DATA_AVAILABILITY, REGULAR_EXPRESSION) ") + 
      UobUtils.ltrim("VALUES({0, number, #}, ''{1}'', ''{2}'', ''{3}'', {4, number, #}, {5, number, #}, {6, number, #}, ''{7}'', ''{8}'', ''{9}'', {10, number, #}, {11, number, #}, ''{12}'', ''{13}'', ") +
      UobUtils.ltrim("       ''{14}'', DEFAULT, ''{15}'', {16}, {17}, {18}, {19}, {20}, {21}, {22}, {23}, {24}, {25}, {26}, {27, number, #}, {28}, {29}, {30}, {31}, {32}, {33}, {34}); ")
	);
	
	private static final MessageFormat INSERT_FIELD_NAME_PATTERNS_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_FIELD_NAME_PATTERNS (FILE_ID, FLD_NM, RCRD_TYP_CD, PROC_ID, FIELD_NAME_PATTERN, CRT_DT, CRT_USR_NM ) VALUES (") +
			UobUtils.ltrim("{0, number, #}, ''{1}'', ''{2}'', ''{3}'', ''{4}'', DEFAULT, ''{5}''); "));
	
	protected Logger logger = Logger.getLogger(getClass());
	
  private int fileId; // Indicates the uniquely generated file identifier
  private RecordType recordTypeInd = RecordType.FIELD_INFO; // Indicates the type of field - D (Data), H (Header), F (Footer)
  private String fieldName; // Name of the field
  private String fieldDesc; // Business Description of the field
  private int fieldNum; // Position of the Field
  private int length; // Length of the field
  private String dataType; // Data Type of the field
  private int decimalPrecision; // Decimal Precision of the field
  private String decimalIndicator; // Decimal Indicator - E (Explicit), I(Implicit)
  private int startPosition; // Start Position of the field (For Fixed Files)
  private int endPosition; // End Position of the field (For Fixed files)
  private String dataFormat; // Format of the Data - Date Formats,etc
  private String defaultValue; // Default Value of the Data
  private char isHashSumField; // Indicates if the field is eligible for Hash Sum Validation
  private char isIndexField; // Indicates if the field is required to be indexed in Solr
  private char isProfileField; // Indicates if the field is eligible for Data Profiling
  private List<Integer> rulesList; // List of Rules to be applied to the field
  private String userNm; // User Name
  private String fieldValue; // Value of the Field
	private String optionality;
	private String bizTerm;
	private String bizDefinition;
	private String synonyms;
	private String usageContext;
	private String systemSteward;
	private String sourceSystem;
	private String sourceTable;
	private String sourceFieldName;
	private String sourceFieldDesc;
	private String sourceFieldType;
	private int sourceFieldLength;
	private String sourceFieldFormat;
	private String sourceDataCategory;
	private String lovCodeAndDesc;
	private String optionality2;
	private String sysdataValidationLogic;
	private String dataAvailability;
	private String excelHeaderMappingField;
	// EDF-214
	private String fieldRegExpression;
	
	/**
   * This method is used to parse a fixed width line.
   * @param line The fixed width line to be parsed
   * @param fldModel The Field Model with the metadata of the start and end positions
   * @throws Exception when there is an error parsing the line
   */
  public void parseFixedWidthLine(String line) throws EDAGValidationException {
  	if (StringUtils.isBlank(line) && (getRecordType().isHeader() || getRecordType().isFooter())) {
  		String type = getRecordType().isHeader() ? "header" : "footer";
  		throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, type,
  				                              "There is no " + type + " in your file. It is probably empty but as it is declared as header/footer, they need to be present even if there is no data rows.");
  	}
  	
    int startPosition = getStartPosition();
    int endPosition = getEndPosition();
    
    if (line.length() < startPosition || line.length() < endPosition) {
      throw new EDAGValidationException("Length of Record in Control Line (" + line.length() + 
      		                             ") is less than expected (" + endPosition + ")");
    }

    String fieldValue = line.substring(startPosition, endPosition);
    if (fieldValue != null) {
      fieldValue = fieldValue.trim();
    }
    
    setFieldValue(fieldValue);
  }
  
  public boolean validateRecordTypeIndicator() throws EDAGValidationException {
  	return validateRecordTypeIndicator(true); // this method will never return false, since it'll throw an exception if the record type indicator is not valid
  }
  
  public boolean validateRecordTypeIndicatorNoException() {
  	try {
			return validateRecordTypeIndicator(false);
		} catch (EDAGValidationException e) {
			// should never reach this block since we instruct the method not to throw exception
			return false;
		}
  }
  
  private boolean validateRecordTypeIndicator(boolean throwException) throws EDAGValidationException {
		// EDF-23
		if (this.isRecordTypeIndicatorField()) {
			String fieldValue = this.getFieldValue();
			if (fieldValue == null) {
				if (throwException) {
					throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, "Record type indicator", "Header / footer indicator must be specified");
				} else {
					return false;
				}
			}
			
			String expectedValue = StringUtils.isEmpty(this.getDefaultValue()) ? this.getRecordType().getDefaultIndicator() 
					                                                               : this.getDefaultValue();
			
			if (!fieldValue.equalsIgnoreCase(expectedValue)) {
				// don't straightaway throw exception. There have been cases where default value is numeric, and it's read from
				// Excel cell as double, then converted to String. This makes '9' default value to be read as '9.0'.
				// So try to convert everything as double value first, then perform double comparison.
				try {
					double numericFieldValue = Double.parseDouble(fieldValue);
					double numericExpectedValue = Double.parseDouble(expectedValue);
					
					if (numericFieldValue != numericExpectedValue) {
						throw new NumberFormatException(); // just throw NumberFormatException to be caught by the catch block
					}
				} catch (NumberFormatException e) {
					// either fieldValue or expectedValue cannot be parsed as double. Throw exception
					if (throwException) {
						throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, fieldValue, "Valid record type indicator is " + expectedValue);
					} else {
						return false;
					}
				}
			}
		}
		
		return true;
	}
  
  /**
   * This method is used to parse a delimited line.
   * @param line The delimited line to be parsed
   * @param colDel The column Delimiter
   * @param txtDel The Text Delimiter
   * @param fldModel The Field Model with the metadata of the columns
   * @throws Exception when there is an error parsing the line
   */
  public void parseDelimitedLine(String line, String colDel, String txtDel) throws EDAGValidationException {
  	if (StringUtils.isBlank(line) && (getRecordType().isHeader() || getRecordType().isFooter())) {
  		String type = getRecordType().isHeader() ? "header" : "footer";
  		throw new EDAGValidationException(EDAGValidationException.NULL_VALUE, type,
  				                              "There is no " + type + " in your file. It is probably empty but as it is declared as header/footer, they need to be present even if there is no data rows.");
  	}
  	
    int fieldNum = getFieldNum() - 1;
    String[] fieldValArray = line.split(colDel, -1);
    if (fieldValArray.length < getFieldNum()) {
      throw new EDAGValidationException("Number of fields in Control Line (" + fieldValArray.length + 
      		                             ") is less than expected (" + getFieldNum() + ")");
    }
    
    String fieldValue = fieldValArray[fieldNum];
    if (fieldValue != null) {
      fieldValue = fieldValue.trim();
    }
    
    if (StringUtils.isBlank(txtDel)) {
      setFieldValue(fieldValue);
    } else {
      int txtDelLen = txtDel.length();
      int fldLen = fieldValue.length();
      fieldValue = fieldValue.substring(txtDelLen, fldLen - txtDelLen);
      setFieldValue(fieldValue);
    }
  }
	
	public String getDataAvailability() {
		return this.dataAvailability;
	}
	
	public void setDataAvailability(String avail) {
		this.dataAvailability = avail == null ? null : normalize(avail.trim());
	}
	
	public String getSysdataValidationLogic() {
		return this.sysdataValidationLogic;
	}
	
	public void setSysdataValidationLogic(String logic) {
		this.sysdataValidationLogic = logic == null ? null : normalize(logic.trim());
	}
	
	public String getOptionality2() {
		return this.optionality2;
	}
	
	public void setOptionality2(String opt) {
		this.optionality2 = opt == null ? null : normalize(opt.trim());
	}
	
	public String getLOVCodeAndDesc() {
		return this.lovCodeAndDesc;
	}
	
	public void setLOVCodeAndDesc(String code) {
		this.lovCodeAndDesc = code == null ? null : normalize(code.trim());
	}
	
	public String getSourceFieldFormat() {
		return this.sourceFieldFormat;
	}
	
	public void setSourceFieldFormat(String format) {
		this.sourceFieldFormat = format == null ? null : normalize(format.trim());
	}
	
	public String getSourceDataCategory() {
		return this.sourceDataCategory;
	}
	
	public void setSourceDataCategory(String cat) {
		this.sourceDataCategory = cat == null ? null : normalize(cat.trim());
	}
	
	public String getBizTerm() {
		return this.bizTerm;
	}
	
	public void setBizTerm(String term) {
		this.bizTerm = term == null ? null : normalize(term.trim());
	}
	
	public String getBizDefinition() {
		return this.bizDefinition;
	}
	
	public void setBizDefinition(String def) {
		this.bizDefinition = def == null ? null : normalize(def.trim());
	}
	
	public String getSynonyms() {
		return this.synonyms;
	}
	
	public void setSynonyms(String syn) {
		this.synonyms = syn == null ? null : normalize(syn.trim());
	}
	
	public String getUsageContext() {
		return this.usageContext;
	}
	
	public void setUsageContext(String ctx) {
		this.usageContext = ctx == null ? null : normalize(ctx.trim());
	}
	
	public String getSystemSteward() {
		return this.systemSteward;
	}
	
	public void setSystemSteward(String steward) {
		this.systemSteward = steward == null ? null : normalize(steward.trim());
	}
	
	public String getSourceSystem() {
		return this.sourceSystem;
	}
	
	public void setSourceSystem(String src) {
		this.sourceSystem = src == null ? null : normalize(src.trim());
	}
	
	public String getSourceTable() {
		return this.sourceTable;
	}
	
	public void setSourceTable(String src) {
		this.sourceTable = src == null ? null : normalize(src.trim());
	}
	
	public String getSourceFieldName() {
		return this.sourceFieldName;
	}
	
	public void setSourceFieldName(String name) {
		this.sourceFieldName = name == null ? null : normalize(name.trim());
	}
	
	public String getSourceFieldDesc() {
		return this.sourceFieldDesc;
	}
	
	public void setSourceFieldDesc(String desc) {
		this.sourceFieldDesc = desc == null ? null : normalize(desc.trim());
	}
	
	public String getSourceFieldType() {
		return this.sourceFieldType;
	}
	
	public void setSourceFieldType(String type) {
		this.sourceFieldType = type == null ? null : normalize(type.trim());
	}
	
	public int getSourceFieldLength() {
		return this.sourceFieldLength;
	}
	
	public void setSourceFieldLength(int length) {
		this.sourceFieldLength = length;
	}
	
	public String getFieldRegExpression() {
		return fieldRegExpression;
	}

	public void setFieldRegExpression(String fieldRegExpression) {
		this.fieldRegExpression = fieldRegExpression;
	}
  
  public List<String> getInsertFieldStdRulesSql() {
  	List<String> result = new ArrayList<String>();
  	for (int rule : this.getRulesList()) {
  			result.add(INSERT_FIELD_STD_RULES_TEMPLATE.format(new Object[] {getFileId(), getFieldName(),
        																												 			  getRecordTypeInd(), rule, 
        																												 			  UobConstants.SYS_USER_NAME}));
  	}		
  	
  	logger.debug("Insert field std rules statement: " + result);
  	return result;
  }
  
  public String getInsertFieldDetailSql() {
  	String result = INSERT_FIELD_DETAIL_TEMPLATE.format(new Object[] {getFileId(), getRecordTypeInd(),
    																													 				getFieldName(), getFieldDesc(),
    																													 				getFieldNum(), getLength(),
    																													 				getDecimalPrecision(), getDataType(),
    																													 				getDataFormat(), getDefaultValue(),
    																													 				getStartPosition(), getEndPosition(),
    																													 				UobUtils.toBooleanChar(isHashSumField()),
    																													 				UobUtils.toBooleanChar(isIndexField()),
    																													 				UobUtils.toBooleanChar(isProfileField()),
    																													 				UobConstants.SYS_USER_NAME,
    																													 				UobUtils.quoteValue(getOptionality()),
    																													 				UobUtils.quoteValue(getBizTerm()),
    																													 				UobUtils.quoteValue(getBizDefinition()),
    																													 				UobUtils.quoteValue(getSynonyms()),
    																													 				UobUtils.quoteValue(getUsageContext()),
    																													 				UobUtils.quoteValue(getSystemSteward()),
    																													 				UobUtils.quoteValue(getSourceSystem()),
    																													 				UobUtils.quoteValue(getSourceTable()),
    																													 				UobUtils.quoteValue(getSourceFieldName()),
    																													 				UobUtils.quoteValue(getSourceFieldDesc()),
    																													 				UobUtils.quoteValue(getSourceFieldType()),
    																													 				getSourceFieldLength(),
    																													 				UobUtils.quoteValue(getSourceFieldFormat()),
    																													 				UobUtils.quoteValue(getSourceDataCategory()),
    																													 				UobUtils.quoteValue(getLOVCodeAndDesc()),
    																													 				UobUtils.quoteValue(getOptionality2()),
    																													 				UobUtils.quoteValue(getSysdataValidationLogic()),
    																													 				UobUtils.quoteValue(getDataAvailability()),
    																													 				UobUtils.quoteValue(getFieldRegExpression())});
  	logger.debug("Insert field detail statement: " + result);
  	return result;
  }
  
  public String getInsertFieldNamePatternsSql(String srcSystem, String procId) {
	String excelMappingField = getExcelHeaderMappingField();
	String result = UobConstants.EMPTY;
	if(!StringUtils.isEmpty(excelMappingField)) {
		result = INSERT_FIELD_NAME_PATTERNS_TEMPLATE.format(new Object[] { getFileId(), getFieldName(),
 				  getRecordTypeInd(), procId, getExcelHeaderMappingField(), UobConstants.SYS_USER_NAME });
		logger.debug("Insert field name patterns statement: " + result);
	}
	return result;
  }
  
  public RecordType getRecordType() {
  	return this.recordTypeInd;
  }
  
  public boolean equals(Object obj) {
  	boolean result = obj instanceof FieldModel;
  	
  	if (result) {
  		FieldModel o = (FieldModel) obj;
  		
  		result = this.fileId == o.fileId &&
  				     this.recordTypeInd == o.recordTypeInd &&
  				     StringUtils.equals(this.fieldName, o.fieldName) && 
  				     StringUtils.equals(this.fieldDesc, o.fieldDesc) &&
  				     this.fieldNum == o.fieldNum &&
  				     this.length == o.length &&
  				     StringUtils.equals(this.dataType, o.dataType) &&
  				     this.decimalPrecision == o.decimalPrecision &&
  				     StringUtils.equals(this.decimalIndicator, o.decimalIndicator) &&
  				     this.startPosition == o.startPosition &&
  				     this.endPosition == o.endPosition &&
  				     StringUtils.equals(this.dataFormat, o.dataFormat) &&
  				     StringUtils.equals(this.defaultValue, o.defaultValue) &&
  				     this.isHashSumField == o.isHashSumField &&
  				     this.isIndexField == o.isIndexField &&
  				     this.isProfileField == o.isProfileField &&
  				     ListUtils.isEqualList(this.rulesList, o.rulesList) &&
  				     StringUtils.equals(this.userNm, o.userNm) &&
  				     StringUtils.equals(this.fieldValue, o.fieldValue) &&
  				     this.optionality == o.optionality &&
  				     StringUtils.equals(this.bizTerm, o.bizTerm) &&
  				     StringUtils.equals(this.bizDefinition, o.bizDefinition) &&
  				     StringUtils.equals(this.synonyms, o.synonyms) &&
  				     StringUtils.equals(this.usageContext, o.usageContext) &&
  				     StringUtils.equals(this.systemSteward, o.systemSteward) &&
  				     StringUtils.equals(this.sourceSystem, o.sourceSystem) &&
  				     StringUtils.equals(this.sourceTable, o.sourceTable) &&
  				     StringUtils.equals(this.sourceFieldName, o.sourceFieldName) &&
  				     StringUtils.equals(this.sourceFieldDesc, o.sourceFieldDesc) &&
  				     StringUtils.equals(this.sourceFieldType, o.sourceFieldType) &&
  				     this.sourceFieldLength == o.sourceFieldLength &&
  				     StringUtils.equals(this.sourceFieldFormat, o.sourceFieldFormat) &&
  				     StringUtils.equals(this.sourceDataCategory, o.sourceDataCategory) &&
  				     StringUtils.equals(this.lovCodeAndDesc, o.lovCodeAndDesc) &&
  				     StringUtils.equals(this.optionality2, o.optionality2) &&
  				     StringUtils.equals(this.sysdataValidationLogic, o.sysdataValidationLogic) &&
  				     StringUtils.equals(this.dataAvailability, o.dataAvailability);
  	}
  	
  	return result;
  }
  
  public int hashCode() {
  	return new HashCodeBuilder().append(this.fileId)
  			                        .append(this.recordTypeInd)
  			                        .append(this.fieldName)
  			                        .append(this.fieldDesc)
  			                        .append(this.fieldNum)
  			                        .append(this.length)
  			                        .append(this.dataType)
  			                        .append(this.decimalPrecision)
  			                        .append(this.decimalIndicator)
  			                        .append(this.startPosition)
  			                        .append(this.endPosition)
  			                        .append(this.dataFormat)
  			                        .append(this.defaultValue)
  			                        .append(this.isHashSumField)
  			                        .append(this.isIndexField)
  			                        .append(this.isProfileField)
  			                        .append(this.rulesList)
  			                        .append(this.userNm)
  			                        .append(this.fieldValue)
  			                        .append(this.optionality)
  			                        .append(this.bizTerm)
  			                        .append(this.bizDefinition)
  			                        .append(this.synonyms)
  			                        .append(this.usageContext)
  			                        .append(this.systemSteward)
  			                        .append(this.sourceSystem)
  			                        .append(this.sourceTable)
  			                        .append(this.sourceFieldName)
  			                        .append(this.sourceFieldDesc)
  			                        .append(this.sourceFieldType)
  			                        .append(this.sourceFieldLength)
  			                        .append(this.sourceFieldFormat)
  			                        .append(this.sourceDataCategory)
  			                        .append(this.lovCodeAndDesc)
  			                        .append(this.optionality2)
  			                        .append(this.sysdataValidationLogic)
  			                        .append(this.dataAvailability)
  			                        .toHashCode();
  }

  public int getFieldNum() {
    return fieldNum;
  }

  public void setFieldNum(int fieldNum) {
    this.fieldNum = fieldNum;
  }
  
  public String getFieldValue() {
    return this.fieldValue;
  }

  public void setFieldValue(String fieldValue) {
    this.fieldValue = fieldValue == null ? null : fieldValue.trim();
  }

  public String getUserNm() {
    return userNm;
  }

  public void setUserNm(String userNm) {
    this.userNm = userNm == null ? null : userNm.trim();
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue == null ? null : defaultValue.trim();
  }

  public List<Integer> getRulesList() {
    return rulesList;
  }

  public void setRulesList(List<Integer> rulesList) {
    this.rulesList = rulesList;
  }

  public int getFileId() {
    return fileId;
  }

  public void setFileId(int fileId) {
    this.fileId = fileId;
  }

  public String getRecordTypeInd() {
    return getRecordType().toString();
  }
  
  public void setRecordTypeInd(String recordTypeInd) throws EDAGValidationException {
  	setRecordType(RecordType.fromString(recordTypeInd));
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName == null ? null : fieldName.trim();
  }

  public String getFieldDesc() {
    return fieldDesc;
  }
  
  protected String normalize(String input) {
  	return input == null ? null : input.replaceAll(UobConstants.QUOTE, UobConstants.EMPTY)
				 															 .replace(UobConstants.BACKSLASH, UobConstants.SPACE)
				 															 .replaceAll(UobConstants.NEWLINE, UobConstants.SPACE)
				 															 .replaceAll(UobConstants.CR, UobConstants.SPACE);
  }
  
  public String getNormalizedFieldDesc() {
  	return normalize(getFieldDesc());
  }
  
  public void setFieldDesc(String fieldDesc) {
    this.fieldDesc = fieldDesc == null ? null : fieldDesc.trim();
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType == null ? null : dataType.trim();
  }

  public int getDecimalPrecision() {
    return decimalPrecision;
  }

  public void setDecimalPrecision(int decimalPrecision) {
    this.decimalPrecision = decimalPrecision;
  }

  public String getDecimalIndicator() {
    return decimalIndicator;
  }

  public void setDecimalIndicator(String decimalIndicator) {
    this.decimalIndicator = decimalIndicator == null ? null : decimalIndicator.trim();
  }

  public int getStartPosition() {
    return startPosition;
  }

  public void setStartPosition(int startPosition) {
    this.startPosition = startPosition;
  }

  public int getEndPosition() {
    return endPosition;
  }

  public void setEndPosition(int endPosition) {
    this.endPosition = endPosition;
  }

  public String getDataFormat() {
    return dataFormat;
  }

  public void setDataFormat(String dataFormat) {
    this.dataFormat = dataFormat == null ? null : dataFormat.trim();
  }
  
  public String getOptionality() {
  	return this.optionality;
  }
  
  public void setOptionality(String optionality) {
  	this.optionality = optionality;
  }
  
  /*
   * This method is provided just for myBatis
   */
  public void setOpt(String opt) {
  	setOptionality(StringUtils.trimToNull(opt));
  }

  public boolean isHashSumField() { 
  	return charToBoolean(isHashSumField); 
  }

  public void setHashSumField(boolean isHashSumField) { 
  	this.isHashSumField = booleanToChar(isHashSumField); 
  }

  public boolean isIndexField() { 
  	return charToBoolean(isIndexField);
  }

  public void setIndexField(boolean isIndexField) { 
  	this.isIndexField = booleanToChar(isIndexField); 
  }

  public boolean isProfileField() { 
  	return charToBoolean(isProfileField); 
  }
  
  public boolean isRecordTypeIndicatorField() {
  	return UobConstants.RECORD_TYPE_IND_DESC_1.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isBusinessDateField() {
  	return UobConstants.BIZ_DATE_DESC_1.equalsIgnoreCase(getFieldDesc()) || 
    			 UobConstants.BIZ_DATE_DESC_2.equalsIgnoreCase(getFieldDesc()) ||
    			 UobConstants.BIZ_DATE_DESC_3.equalsIgnoreCase(getFieldDesc()) ||
    			 UobConstants.BIZ_DATE_DESC_1.equalsIgnoreCase(getFieldName()) ||
    			 UobConstants.BIZ_DATE_DESC_2.equalsIgnoreCase(getFieldName()) ||
           UobConstants.BIZ_DATE_DESC_3.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isSourceSystemField() {
  	return UobConstants.SRC_SYS_DESC_1.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.SRC_SYS_DESC_2.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.SRC_SYS_DESC_1.equalsIgnoreCase(getFieldName()) ||
        	 UobConstants.SRC_SYS_DESC_2.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isCountryField() {
  	return UobConstants.CTRY_DESC_1.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.CTRY_DESC_2.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.CTRY_DESC_1.equalsIgnoreCase(getFieldName()) ||
        	 UobConstants.CTRY_DESC_2.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isFileNameField() {
  	return UobConstants.FILE_NM_DESC_1.equalsIgnoreCase(getFieldDesc()) || 
    			 UobConstants.FILE_NM_DESC_2.equalsIgnoreCase(getFieldDesc()) ||
    			 UobConstants.FILE_NM_DESC_1.equalsIgnoreCase(getFieldName()) ||
    			 UobConstants.FILE_NM_DESC_2.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isRecordCountField() {
  	return UobConstants.RCRD_CNT_DESC_1.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.RCRD_CNT_DESC_2.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.RCRD_CNT_DESC_3.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.RCRD_CNT_DESC_1.equalsIgnoreCase(getFieldName()) ||
        	 UobConstants.RCRD_CNT_DESC_2.equalsIgnoreCase(getFieldName()) ||
        	 UobConstants.RCRD_CNT_DESC_3.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isHashsumColumnField() {
  	return UobConstants.HASH_COL_DESC_1.equalsIgnoreCase(getFieldDesc()) ||
        	 UobConstants.HASH_COL_DESC_1.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isHashsumValueField() {
  	return UobConstants.HASH_SUM_DESC_1.equalsIgnoreCase(getFieldName()) || 
    			 UobConstants.HASH_SUM_DESC_2.equalsIgnoreCase(getFieldName()) ||
    			 UobConstants.HASH_SUM_DESC_1.equalsIgnoreCase(getFieldDesc()) ||
    			 UobConstants.HASH_SUM_DESC_2.equalsIgnoreCase(getFieldDesc()) ||
    			 UobConstants.HASH_SUM_DESC_3.equalsIgnoreCase(getFieldName());
  }
  
  public boolean isSystemDateField() {
  	return UobConstants.SYS_DATE_DESC_1.equalsIgnoreCase(getFieldName()) || 
    			 UobConstants.SYS_DATE_DESC_1.equalsIgnoreCase(getFieldDesc());
  }

  public void setProfileField(boolean isProfileField) { 
  	this.isProfileField = booleanToChar(isProfileField); 
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("fileID", fileId)
    		                            .append("recordTypeInd", recordTypeInd)
    		                            .append("fieldName", fieldName)
    		                            .append("fieldDesc", fieldDesc)
    		                            .append("fieldNum", fieldNum)
    		                            .append("length", length)
    		                            .append("dataType", dataType)
    		                            .append("decimalPrecision", decimalPrecision)
    		                            .append("decimalIndicator", decimalIndicator)
    		                            .append("startPosition", startPosition)
    		                            .append("endPosition", endPosition)
    		                            .append("dataFormat", dataFormat)
    		                            .append("defaultValue", defaultValue)
    		                            .append("isHashSumField", isHashSumField)
    		                            .append("isIndexField", isIndexField)
    		                            .append("isProfileField", isProfileField)
    		                            .append("rulesList", rulesList)
    		                            .append("userNm", userNm)
    		                            .append("fieldValue", fieldValue)
    		                            .append("optionality", optionality)
    		                            .append("bizTerm", bizTerm)
    		                            .append("bizDefinition", bizDefinition)
    		                            .append("synonyms", synonyms)
    		                            .append("usageContext", usageContext)
    		                            .append("systemSteward", systemSteward)
    		                            .append("sourceSystem", sourceSystem)
    		                            .append("sourceTable", sourceTable)
    		                            .append("sourceFieldName", sourceFieldName)
    		                            .append("sourceFieldDesc", sourceFieldDesc)
    		                            .append("sourceFieldType", sourceFieldType)
    		                            .append("sourceFieldLength", sourceFieldLength)
    		                            .append("sourceFieldFormat", sourceFieldFormat)
    		                            .append("sourceDataCategory", sourceDataCategory)
    		                            .append("lovCodeAndDescription", lovCodeAndDesc)
    		                            .append("optionality2", optionality2)
    		                            .append("sysdataValidationLogic", sysdataValidationLogic)
    		                            .append("dataAvailability", dataAvailability)
    		                            .toString();
  }

  /**
   * This method is used to clone the Field Model object to a new object instance.
   */
  public FieldModel clone() {
    FieldModel clone = new FieldModel();
    
    clone.dataFormat = this.dataFormat;
    clone.dataType = this.dataType;
    clone.decimalIndicator = this.decimalIndicator;
    clone.decimalPrecision = this.decimalPrecision;
    clone.defaultValue = this.defaultValue;
    clone.endPosition = this.endPosition;
    clone.fieldDesc = this.fieldDesc;
    clone.fieldName = this.fieldName;
    clone.fieldNum = this.fieldNum;
    clone.fieldValue = this.fieldValue;
    clone.fileId = this.fileId;
    clone.isHashSumField = this.isHashSumField;
    clone.isIndexField = this.isIndexField;
    clone.isProfileField = this.isProfileField;
    clone.length = this.length;
    clone.recordTypeInd = this.recordTypeInd;
    if (this.rulesList != null) {
	    clone.rulesList = new ArrayList<Integer>();
	    clone.rulesList.addAll(this.rulesList);
    }
    clone.startPosition = this.startPosition;
    clone.userNm = this.userNm;
    clone.optionality = this.optionality;
    clone.bizTerm = this.bizTerm;
    clone.bizDefinition = this.bizDefinition;
    clone.synonyms = this.synonyms;
    clone.usageContext = this.usageContext;
    clone.systemSteward = this.systemSteward;
    clone.sourceSystem = this.sourceSystem;
    clone.sourceTable = this.sourceTable;
    clone.sourceFieldName = this.sourceFieldName;
    clone.sourceFieldDesc = this.sourceFieldDesc;
    clone.sourceFieldType = this.sourceFieldType;
    clone.sourceFieldLength = this.sourceFieldLength;
    clone.sourceFieldFormat = this.sourceFieldFormat;
    clone.sourceDataCategory = this.sourceDataCategory;
    clone.lovCodeAndDesc = this.lovCodeAndDesc;
    clone.optionality2 = this.optionality2;
    clone.sysdataValidationLogic = this.sysdataValidationLogic;
    clone.dataAvailability = this.dataAvailability;
    
    return clone;
  }
  
  /**
   *
   * Workaround as Oracle does not have boolean type but rely on char or integer
   *
   * @param c char
   * @return boolean
   */
  private static boolean charToBoolean(char c) {
    return (c == 'Y') ? true : false;
  }

  /**
   *
   * Workaround as Oracle does not have boolean type but rely on char or integer
   *
   * @param b boolean
   * @return char
   */
  private static char booleanToChar(boolean b) {
    return b ? 'Y' : 'N';
  }

	public void setRecordType(RecordType recordType) {
		this.recordTypeInd = recordType;
	}
	
	public String getNormalizedFieldName() {
		return com.uob.edag.utils.StringUtils.normalizeForHive(getFieldName());
	}
	
	public void setExcelHeaderMappingField(String excelHeaderMappingField) {
		this.excelHeaderMappingField = excelHeaderMappingField == null ? null : StringUtils.trim(excelHeaderMappingField);
	}
	
	public String getExcelHeaderMappingField() {
		return this.excelHeaderMappingField;
	}

	/*
	public String getDefaultNormalizedReferencedFileFieldName() {
		return getNormalizedFieldName() + DEFAULT_REFERENCED_FILE_FIELD_NAME_SUFFIX;
	}
	
	public String getNormalizedReferencedFileFieldName(ProcessModel procModel) {
		String suffix = StringUtils.trimToNull(procModel.getProcParam(UobConstants.PROC_PARAM_REFERENCED_FILE_FIELDNAME_SUFFIX));
		return suffix == null ? getDefaultNormalizedReferencedFileFieldName() : getNormalizedFieldName() + suffix;
	}
	*/
	@Override
	public int compareTo(FieldModel o) {
		int result = o == null ? 1 : 0;
		
		if (result == 0) {
			result = Integer.compare(this.fileId, o.fileId);
		}
		
		if (result == 0) {
			result = Integer.compare(this.recordTypeInd.getRanking(), o.recordTypeInd.getRanking());
		}
		
		if (result == 0) {
			result = Integer.compare(this.fieldNum, o.fieldNum);
		}
		
		return result;
	}
}
