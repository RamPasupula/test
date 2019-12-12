package com.uob.edag.constants;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.uob.edag.utils.PropertyLoader;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/21/2016
 * @Description : The file contains the commonly used constants used by this
 *              project.
 *
 */

public class UobConstants {

  // PII Registration
  public static final String ENABLE_SUPPRESS_HQL_FIELD = ".ENABLE_SUPPRESS_HQL_FIELD";
  public static final String BATCH_NUM = ".BATCH_NUM";
  public static final String PII_FLAG = ".PII_FLAG";
  public static final String SUPPRESSED_HQL_FIELDS = ".SUPPRESSED_HQL_FIELDS";
  public static final String SQL_FILE_DDS_NAME = ".SQL_FILE_DDS_NAME";
  public static final String JDBC_DRIVER = "JDBC.Driver";
  public static final String JDBC_CONNECTION_URL = "JDBC.ConnectionURL";
  public static final String JDBC_USERNAME = "JDBC.Username";
  public static final String VIEW_TEMPLATE = ".VIEW_TEMPLATE";
  public static final String PS_DELETE_EXISTING_PII = "DELETE FROM EDAG_FIELD_SENSITIVE_DETAIL WHERE (src_sys_nm = ? AND file_nm = ?)";
  public static final String PS_DELETE_EXISTING_PII_WITHOUT_FILENAME = "DELETE FROM EDAG_FIELD_SENSITIVE_DETAIL WHERE src_sys_nm = ?";
  public static final String PS_INSERT_SENSITIVE_TEMPLATE = "INSERT INTO EDAG_FIELD_SENSITIVE_DETAIL ( batch_num,src_sys_nm,hive_tbl_nm,file_nm,fld_nm  ) VALUES (?,?,?,?,?)";
  public static final String PS_PRECHECK_EXISTS_PII = "SELECT 1 FROM EDAG_FIELD_SENSITIVE_DETAIL WHERE src_sys_nm = ?";
  public static final int FILE_SPEC_FILE_DESCRIPTION_COL_NUM = 2;
  public static final int FILE_SPEC_FILE_TYPE_COL_NUM = 4;
  public static final int FILE_SPEC_FILE_NAME_COL_NUM = 1;
  public static final String FILE_NAME_ROW_TYPE_FIELD_VALUE_HEADER = "HR";
  public static final String FILE_NAME_ROW_TYPE_FIELD_VALUE_FOOTER = "FR";
  public static final int FILE_NAME_FIELD_NAME_COL_NUM = 1;
  public static final int FILE_NAME_ROW_TYPE_COL_NUM = 5;
  public static final String NULL_TAG = "<NULL>";
  public static final String CTRY = "CTRY";
  public static final String IS_TRUE ="true";
  public static final String NS_VIEWS_OPTION = "ns_views";
  public static final String S_VIEWS_OPTION = "s_views";
  public static final String CURRENT_TIMESTAMP = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date());
  public static final String ENABLE_SQL_COMMENTS = ".ENABLE_SQL_COMMENTS";
  public static final String FILENAME_CTRY_REGEX = ".FILENAME_CTRY_REGEX";
  public static final String GENERATE_CLEANUP_SCRIPT = ".GENERATE_CLEANUP_SCRIPT";
  public static final String CLEANUP_FILE_NAME = "dds_registration_cleanup.sql";

  // Swift
  public static final String SUFFIX_TEMP = "_TEMP";

  // Property Names
  public static final String DB_PROPERTIES = "DB_PROPERTIES";
  public static final String KERBEROS_ENABLED = "KERBEROS_ENABLED";
  public static final String KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";
  public static final String KERBEROS_KEYTAB_FILE = "KERBEROS_KEYTAB_FILE";
  public static final String KERBEROS_CONF_FILES = "KERBEROS_CONF_FILES";
  public static final String HADOOP_HOME = "HADOOP_HOME";
  public static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
  public static final String HADOOP_CLASSPATH = "HADOOP_CLASSPATH";
  public static final String SCRIPT_DIR = "SCRIPT_DIR";
  public static final String NODE1 = "NODE1";
  public static final String NODE2 = "NODE2";
  public static final String NODE3 = "NODE3";
  public static final String NODE1_USER = "NODE1_USER";
  public static final String NODE2_USER = "NODE2_USER";
  public static final String NODE3_USER = "NODE3_USER";
  public static final String NODE1_PWD = "NODE1_PWD";
  public static final String NODE2_PWD = "NODE2_PWD";
  public static final String NODE3_PWD = "NODE3_PWD";
  public static final String EXCEL_COLUMN_VALUES_TO_IGNORE = "EXCEL_COLUMN_VALUES_TO_IGNORE";
  public static final String EXCEL_HEADER_FIELD_SPECIAL_CHARACTER_REPLACE_REGEX_EXPRESSION = "EXCEL_HEADER_FIELD_SPECIAL_CHARACTER_REPLACE_REGEX_EXPRESSION";
  public static final String COUNTRY_COUNTRL_CHAR = "COUNTRY_COUNTRL_CHAR";

  // Teradata JDBC
  public static final String TERADATA_JDBC_DRIVER = "Teradata.JDBC.Driver";
  public static final String TERADATA_JDBC_URL = "Teradata.JDBC.ConnectionURL";
  public static final String TERADATA_JDBC_URL_PARAM_PREFIX = "Teradata.JDBC.ConnectionURL.Param.";
  public static final String TERADATA_JDBC_USER = "Teradata.JDBC.Username";
  public static final String TERADATA_JDBC_USER_TDWALLET = "Teradata.JDBC.Username.TDWallet";
  public static final String TERADATA_JDBC_PASSWORD = "Teradata.JDBC.Password";
  public static final String TERADATA_JDBC_PASSWORD_TDWALLET = "Teradata.JDBC.Password.TDWallet";
  public static final String TERADATA_JDBC_POOL_PING_ENABLED = "Teradata.JDBC.PoolPingEnabled";
  public static final String TERADATA_JDBC_POOL_PING_QUERY = "Teradata.JDBC.PoolPingQuery";

  // Source File Types
  public static final String SRC_FILE_TYPE_VAL_1 = "Master";
  public static final String SRC_FILE_TYPE_VAL_2 = "Parameter";
  public static final String SRC_FILE_TYPE_VAL_3 = "Transaction";
  public static final String SRC_FILE_TYPE_VAL_4 = "Enriched";
  public static final String SRC_FILE_TYPE_VAL_5 = "TechRecon";
  public static final String SRC_FILE_TYPE_VAL_6 = "Log";
  public static final String SRC_FILE_TYPE_VAL_7 = "Control File";
  public static final String SRC_FILE_TYPE_VAL_8 = "Mapping";

  // Source File Layouts
  public static final String SRC_FILE_LAYOUT_VAL_F = "Fixed";
  public static final String SRC_FILE_LAYOUT_VAL_D = "Delimited";

  // Process Types
  public static final String PROC_TYPE_VAL_1 = "File Ingestion";
  public static final String PROC_TYPE_VAL_2 = "Database Ingestion";

  // Record Type Indicators
  public static final String RCRD_TYPE_VAL_D = "Data";
  public static final String RCRD_TYPE_VAL_H = "Header";
  public static final String RCRD_TYPE_VAL_T = "Trailer";
  public static final String RCRD_TYPE_VAL_C = "Control File";

  // Decimal Indicators
  public static final String DECIMAL_IND_VAL_E = "Explicit";
  public static final String DECIMAL_IND_VAL_I = "Implicit";

  // Control Info
  public static final String CTRL_INFO_C = "C";
  public static final String CTRL_INFO_HT = "HT";
  public static final String CTRL_INFO_VAL_C = "Control File";
  public static final String CTRL_INFO_VAL_H = "Header/Trailer";

  // Hadoop Formats
  public static final String FORMAT_VAL_1 = "Parquet";
  public static final String FORMAT_VAL_2 = "ORC";
  public static final String FORMAT_VAL_3 = "Sequence";
  public static final String FORMAT_VAL_4 = "Text";
  public static final String FORMAT_VAL_5 = "Avro";
  public static final String FORMAT_VAL_6 = "RCFile";

  // Hadoop Compressions
  public static final String COMPR_VAL_1 = "Snappy";
  public static final String COMPR_VAL_2 = "ZLib";
  public static final String COMPR_VAL_3 = "Gzip";
  public static final String COMPR_VAL_4 = "Bzip2";
  public static final String COMPR_VAL_5 = "LZO";
  public static final String COMPR_VAL_6 = "LZ4";

  // Load Types / Apply Types
  public static final String LOAD_TYPE_VAL_1 = "Full Replace";
  public static final String LOAD_TYPE_VAL_2 = "Append";

  // Country Codes
  public static final String GLOBAL = "GD";
  public static final String SINGAPORE = "SG";
  public static final String CHINA = "CN";
  public static final String INDONESIA = "ID";
  public static final String MALAYSIA = "MY";
  public static final String THAILAND = "TH";
  public static final String AUSTRALIA = "AU";
  public static final String BRUNEI = "BN";
  public static final String CANADA = "CA";
  public static final String UNITED_KINGDOM = "GB";
  public static final String HONG_KONG = "HK";
  public static final String INDIA = "IN";
  public static final String JAPAN = "JP";
  public static final String SOUTH_KOREA = "KR";
  public static final String MYANMAR = "MM";
  public static final String M1 = "M1";
  public static final String M2 = "M2";
  public static final String PHILLIPINES = "PH";
  public static final String TAIWAN = "TW";
  public static final String U1 = "U1";
  public static final String U2 = "U2";
  public static final String VIETNAM = "VN";
  public static final String CTRY_PREFIX = "CTRY_";
  public static final String CHARSET_PREFIX = "CHARSET_";
  public static final String ENCODING_PREFIX = "ENCODING_";
  public static final String CHARSET_SUFFIX = "_CHARSET";
  public static final String ENCODING_SUFFIX = "_ENCODING";
  public static final String REF_FILE_FOLDER_PREFIX = "REF_FILE_FOLDER_";

  //Don't change the content / order of the country codes randomly, since the order must match the order in the Excel file spec spreadsheet
  public static final String[] COUNTRY_CODES = new String[]{GLOBAL, SINGAPORE, CHINA, INDONESIA, MALAYSIA, THAILAND,
          AUSTRALIA, BRUNEI, CANADA, UNITED_KINGDOM, HONG_KONG, INDIA,
          JAPAN, SOUTH_KOREA, MYANMAR, M2, PHILLIPINES, TAIWAN,
          U1, U2, VIETNAM, M1};

  public static String getCountryInterfaceSpecKey(String countryCode) {
    return CTRY_PREFIX + countryCode;
  }

  public static String getCharsetInterfaceSpecKey(String countryCode) {
    return CHARSET_PREFIX + countryCode;
  }

  public static String getEncodingInterfaceSpecKey(String countryCode) {
    return ENCODING_PREFIX + countryCode;
  }

  public static String getReferencedFileFolderSpecKey(String countryCode) {
    return REF_FILE_FOLDER_PREFIX + countryCode;
  }

  // Frequency Constants
  public static final String FREQ_VAL_H = "Hourly";
  public static final String FREQ_VAL_D = "Daily";
  public static final String FREQ_VAL_W = "Weekly";
  public static final String FREQ_VAL_M = "Monthly";
  public static final String FREQ_VAL_Q = "Quarterly";
  public static final String FREQ_VAL_HY = "Half Yearly";
  public static final String FREQ_VAL_Y = "Yearly";
  public static final String FREQ_VAL_A = "Adhoc";

  // Interface Related Constants
  public static final String INTER_SPEC_FILE_PATTERN = "UOB EDAG - Source_File_Interface_Spec";
  // Sheets
  public static final String SRC_SYS_SPEC = "Source_File_Specification";
  public static final String SRC_FLD_SPEC = "Source_Field_Specification";
  public static final String CTRL_SPEC = "Control_Info_Specification";

  // UOB - Interface Related Constants
  public static final String UOB_SRC_SYS_SPEC = "File Spec";
  public static final String DATA_INGESTION_SPEC = "DataIngestion";
  public static final String DATA_INGEST_PARAM_SPEC = "DataIngestion_Param";
  public static final String DATA_EXPORT_PARAM_SPEC = "DataExport_Param";
  public static final String PROC_DOWNSTREAM_SPEC = "ProcDownStreamApp";

  public static final String DATA_EXPORT_SPEC = "DataExport";
  public static final String FILTER_PARAM_NAME = "FILTER";

  // Excel File Parameter Names
  public static final String EXCEL_FILE_NAME = "EXCEL_FILE_NAME";
  public static final String EXCEL_SHEET_NAME = "EXCEL_SHEET_NAME";
  public static final String EXCEL_BUSINESS_DATE_FORMAT = "EXCEL_FILE_NAME_TOKEN_FORMAT_businessDate";
  public static final String EXCEL_BUSINESS_DATE_DEFAULT_FORMAT = "yyyyMMdd";
  public static final String EXCEL_BUSINESS_DATE_PARAM_NAME = "businessDate";

  // WebLogs Parameter Names
  public static final String WEBLOGS_HDFS_FOLDER_PATH = "HDFS_FOLDER_PATH";
  public static final String WEBLOGS_TOPIC_NAME = "TOPIC_NAME";
  public static final String WEBLOGS_EDAG_LOAD_TABLE_FREQUENCY = "EDAG_LOAD_TABLE_FREQUENCY";
  
  // Compaction Parameter Names
  public static final String COMPACTION_ENABLED = "COMPACTION_ENABLED";
  public static final String TARGET_TABLE_NAME = "TARGET_TABLE_NAME";
  public static final String TARGET_TABLE_PARTITION_INFO = "TARGET_TABLE_PARTITION_INFO";
  public static final String DROP_SOURCE_TABLE_PARTITION = "DROP_SOURCE_TABLE_PARTITION";

  // Interface Spec Fields
  // Source File Spec
  public static final String SNO = "SNO";
  public static final String FILE_NM = "FILE_NM";
  public static final String FILE_DESC = "FILE_DESC";
  public static final String FILE_FREQ = "FILE_FREQ";
  public static final String FILE_TYPE = "FILE_TYPE";
  public static final String FILE_FORMAT = "FILE_FORMAT";
  public static final String HASHSUM_FLD = "HASHSUM_FLD";
  public static final String LOAD_STRTGY = "LOAD_STRTGY";
  public static final String DEC_IND = "DEC_IND";
  public static final String CHARSET = "CHARSET";

  public static final String CONTROL_FILE_PATTERN = "Control file";
  public static final String HASHSUM_FLD_PATTERN = "Sum of ";
  public static final String SNO_PATTERN = "sno";

  // Source Field Spec
  public static final String FIELD_NM = "FIELD_NM";
  public static final String FIELD_NUM = "FIELD_NUM";
  public static final String FIELD_DESC = "FIELD_DESC";
  public static final String FIELD_TYPE = "FIELD_TYPE";
  public static final String FIELD_LEN = "FIELD_LEN";
  public static final String FIELD_START_POS = "FIELD_START_POS";
  public static final String FIELD_END_POS = "FIELD_END_POS";
  public static final String DEC_PREC = "DEC_PREC";
  public static final String RCRD_TYPE = "RCRD_TYPE";
  public static final String MDT_OPT = "MDT_OPT";
  public static final String FORMAT = "FORMAT";
  public static final String DEFAULT_VAL = "DEFAULT_VAL";
  public static final String REMARKS = "REMARKS";

  public static final String PII = "PII";
  public static final String EXCEL_FIELD_HEADER_MAPPING = "EXCEL_FIELD_HEADER_MAPPING";
  public static final String REGULAR_EXPRESSION = "REGEX";
  public static final String BIZ_TERM = "BIZ_TERM";
  public static final String BIZ_DEFINITION = "BIZ_DEFINITION";
  public static final String SYNONYMS = "SYNONYMS";
  public static final String USAGE_CONTEXT = "USAGE_CONTEXT";
  public static final String SYSTEM_STEWARD = "SYSTEM_STEWARD";
  public static final String SOURCE_SYSTEM = "SOURCE_SYSTEM";
  public static final String SOURCE_TABLE = "SOURCE_TABLE";
  public static final String SOURCE_FIELD_NAME = "SOURCE_FIELD_NAME";
  public static final String SOURCE_FIELD_DESC = "SOURCE_FIELD_DESC";
  public static final String SOURCE_FIELD_TYPE = "SOURCE_FIELD_TYPE";
  public static final String SOURCE_FIELD_LENGTH = "SOURCE_FIELD_LENGTH";
  public static final String SOURCE_FIELD_FORMAT = "SOURCE_FIELD_FORMAT";
  public static final String SOURCE_DATA_CATEGORY = "SOURCE_DATA_CATEGORY";
  public static final String LOV_CODE_AND_DESC = "LOV_CODE_AND_DESC";
  public static final String OPTIONALITY_2 = "OPTIONALITY_2";
  public static final String SYSDATA_VALIDATION_LOGIC = "SYSDATA_VALIDATION_LOGIC";
  public static final String DATA_AVAILABILITY = "DATA_AVAILABILITY";

  // Process Spec
  public static final String SRC_SYS_NM = "SRC_SYS_NM";
  public static final String INTERFACE_SPEC_NM = "INTERFACE_SPEC_NM";
  public static final String PROCESS_ID = "PROCESS_ID";
  public static final String PROCESS_NAME = "PROCESS_NAME";
  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String PROCESS_GRP = "PROCESS_GRP";
  public static final String BUSINESS_DT_EXPR = "BUSINESS_DT_EXPR";
  public static final String T1_PARTITION = "T1_PARTITION";
  public static final String T11_PARTITION = "T11_PARTITION";
  public static final String PROCESS_CRITICALITY = "PROCESS_CRITICALITY";
  public static final String DEPLOYMENT_NODE = "DEPLOYMENT_NODE";
  public static final String ALERT_EMAIL = "ALERT_EMAIL";
  public static final String PARAM_NAME = "PARAM_NAME";
  public static final String PARAM_VALUE = "PARAM_VALUE";
  public static final String DOWNSTREAM_APPL = "DOWNSTREAM_APPL";
  public static final String ERROR_THRESHOLD = "ERROR_THRESHOLD";
  public static final String DATA_LOADING = "DataLoading";
  public static final String DATALOADING_PARAM = "DataLoading_Param";
  public static final String PROC_DOWNSTREAM_APP= "ProcDownStreamApp";
  public static final String SERIAL_NUMBER = "Serial Number";
  public static final String PROCESS_ID_STR = "Process ID";

  // Process Spec Param
  public static final String PROC_PARAM_PROCESS_FREQUENCY = "PROCESS_FREQUENCY";
  public static final String PROC_PARAM_CTRL_BIZ_DATE_VALIDATION_FORMAT = "CTRL_BIZ_DATE_VALIDATION_FORMAT";
  public static final String PROC_PARAM_REFERENCED_SOURCE_FOLDER = "referencedfile.source.folder";
  public static final String PROC_PARAM_REFERENCED_TARGET_FOLDER = "referencedfile.target.folder";
  public static final String PROC_PARAM_REFERENCED_FILE_FIELDNAME_SUFFIX = "referencedfile.hdfs.uri.fieldname.suffix";

  // Process Spec - Export Process
  public static final String CTRY_CD = "CTRY_CD";
  public static final String FREQ_CD = "FREQ_CD";
  public static final String SRC_DB_NAME = "SRC_DB_NAME";
  public static final String SRC_TBL_NAME = "SRC_TBL_NAME";
  public static final String TGT_DIR_NAME = "TGT_DIR_NAME";
  public static final String TGT_FILE_NAME = "TGT_FILE_NAME";
  public static final String TGT_FILE_EXTN = "TGT_FILE_EXTN";
  public static final String TGT_COL_DELIM = "TGT_COL_DELIM";
  public static final String TGT_TXT_DELIM = "TGT_TXT_DELIM";
  public static final String CTRL_FILE_NM = "CTRL_FILE_NM";
  public static final String CTRL_FILE_EXTN = "CTRL_FILE_EXTN";

  // Record type indicator field name
  public static final String RECORD_TYPE_IND_DESC_1 = "Record Type";

  // Biz Date Field Descriptions
  public static final String BIZ_DATE_DESC_1 = "EOD BUSINESS DT";
  public static final String BIZ_DATE_DESC_2 = "Business date";
  public static final String BIZ_DATE_DESC_3 = "Cycle Date";

  // Sys Date Field Descriptions
  public static final String SYS_DATE_DESC_1 = "System date & Time";

  // Src Systems Field Descriptions
  public static final String SRC_SYS_DESC_1 = "SOURCE SYSTEM CODE";
  public static final String SRC_SYS_DESC_2 = "Source System ID";

  // Country codes Field Descriptions
  public static final String CTRY_DESC_1 = "COUNTRY CD";
  public static final String CTRY_DESC_2 = "Country Code";

  // File Names Field Descriptions
  public static final String FILE_NM_DESC_1 = "BASE SYS FILE";
  public static final String FILE_NM_DESC_2 = "Base File";

  // Record Count Field Descriptions
  public static final String RCRD_CNT_DESC_1 = "TOT NO OF RECORDS";
  public static final String RCRD_CNT_DESC_2 = "Record count";
  public static final String RCRD_CNT_DESC_3 = "Count";

  // Hash Column Field Descriptions
  public static final String HASH_COL_DESC_1 = "HASH COLUMN 1";

  // Hash Sum Field Descriptions
  public static final String HASH_SUM_DESC_1 = "HASH SUM 1";
  public static final String HASH_SUM_DESC_2 = "Hashsum";
  public static final String HASH_SUM_DESC_3 = "hash total";

  // Character Constants
  public static final String COMMA = ",";
  public static final String ATSIGN = "@";
  public static final String EQUAL = "=";
  public static final String NEWLINE = "\n";
  public static final String CR = "\r";
  public static final String SEPARATOR = "/";
  public static final String COLON = ":";
  public static final String SEMICOLON = ";";
  public static final String SPACE = " ";
  public static final String QUOTE = "'";
  public static final String BACKSLASH = "\\";
  public static final String HASH = "#";
  public static final String DUMMY = "Dummy";
  public static final String EMPTY = "";
  public static final String PIPE = "|";
  public static final String BELL_CHAR = "\\u0007";
  public static final String BELL_DELIMITER_HIVE_CODE = "\\007";
  public static final String UNDERSCORE = "_";
  public static final String DOUBLE_UNDERSCORE = "__";
  public static final String ASTERISK = "*";
  public static final String QUESTION = "?";
  public static final String DOLLAR = "$";
  public static final String DOLLAR_CODE = "\\044";
  public static final String SUBSTITUTE_CHAR = "\u001a";
  public static final String DOLLAR_SUBSTITUTE = "DL";
  public static final String HASH_SUBSTITUTE = "HX";
  public static final String SEPARATOR_SUBSTITUTE = "BL";
  public static final String ARROW = "->";
  public static final String EMPTY_STRING = "";

  // Frequency Constants
  public static final String DAILY = "Daily";
  public static final String WEEKLY = "Weekly";
  public static final String MONTHLY = "Monthly";
  public static final String QUARTERLY = "Quarterly";
  public static final String YEARLY = "Yearly";
  public static final String ADHOC = "Adhoc";
  public static final String HISTORY = "History";
  public static final String CYCLIC = "Cyclic";

  public static final String DAILY_CD = "D";
  public static final String WEEKLY_CD = "W";
  public static final String MONTHLY_CD = "M";
  public static final String QUARTERLY_CD = "Q";
  public static final String YEARLY_CD = "Y";
  public static final String ADHOC_CD = "A";
  public static final String HISTORY_CD = "H";
  public static final String CYCLIC_CD = "C";
  public static final List<String> VALID_RECURRENCES = Collections.unmodifiableList(
          Arrays.asList(DAILY, WEEKLY, MONTHLY, QUARTERLY, YEARLY, ADHOC, HISTORY, CYCLIC));

  // Process Criticality
  public static final String RUSH_PRI = "R";
  public static final String HIGH_PRI = "H";
  public static final String MED_PRI = "M";
  public static final String LOW_PRI = "L";
  public static final List<String> VALID_PRI = Collections.unmodifiableList(
          Arrays.asList(RUSH_PRI, HIGH_PRI, MED_PRI, LOW_PRI));

  // Boolean Constants
  public static final String TRUE = "true";
  public static final String FALSE = "false";
  public static final String Y = "Y";
  public static final String N = "N";

  // Number Constants
  public static final String MINUS_ONE = "-1";
  public static final String ZERO = "0";
  public static final String ONE = "1";
  public static final String TWO = "2";
  public static final String THREE = "3";
  public static final String FOUR = "4";
  public static final String FIVE = "5";
  public static final String SIX = "6";
  public static final String SEVEN = "7";
  public static final String EIGHT = "8";
  public static final String TWENTY_FIVE = "25";

  // Source File Layouts Codes
  public static final String FIXED_FILE = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.FIXED");
  public static final String DELIMITED_FILE = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.DELIMITED");
  public static final String FIXED_TO_DELIMITED_FILE = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.FIXED_TO_DELIMITED");
  public static final String XLS_FILE_WITH_HEADER = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.XLS_WITH_HEADER");
  public static final String XLS_FILE_WITHOUT_HEADER = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.XLS_WITHOUT_HEADER");
  public static final String XLSX_FILE_WITH_HEADER = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.XLSX_WITH_HEADER");
  public static final String XLSX_FILE_WITHOUT_HEADER = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.XLSX_WITHOUT_HEADER");
  public static final String REG_EXPRESSION = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.REG_EXPRESSION");
  public static final String CSV = PropertyLoader.getProperty("com.uob.edag.validation.FileFormat.CSV");

  // Source File Types Codes
  public static final String MASTER_FILE = "MST";
  public static final String PARAMETER_FILE = "PRM";
  public static final String TRANSACTION_FILE = "TXN";
  public static final String ENRICHED_FILE = "ENR";
  public static final String TECHRECON_FILE = "TRC";
  public static final String LOG_FILE = "LOG";
  public static final String INTERFACE_FILE = "INT";
  public static final String CONTROL_FILE = "CTR";
  public static final String MAPPING_FILE = "MAP";
  public static final String REFERENCE_FILE = "REF";
  public static final String WORK_FILE = "WRK";

  // Proc Type Code
  public static final String FILE_INGEST_PROC_TYPE = "21";
  public static final String TPT_EXPORT_PROC_TYPE = "71";

  // Load Type Codes
  public static final String FULL_LOAD_CD = "FLL";
  public static final String APPEND_LOAD_CD = "APD";
  public static final String HISTORY_LOAD_CD = "HST";
  public static final String ADDITIONAL_LOAD_CD = "ADD";

  // Hadoop File Format Codes
  public static final String PARQUET_CD = "PRQ";
  public static final String ORC_CD = "ORC";
  public static final String SEQ_CD = "SEQ";
  public static final String TEXT_CD = "TXT";

  // File Compression Codes
  public static final String SNAPPY_CD = "SNP";
  public static final String ZLIB_CD = "ZLB";
  public static final String GZIP_CD = "GZP";
  public static final String BZIP_CD = "BZP";
  public static final String LZO_CD = "LZO";
  public static final String LZ4_CD = "LZ4";

  // Date & Time Constants
  public static final String OOZIE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";
  public static final String UTC = "UTC";

  // Status ConstantsLayouts
  public static final String SUCCESS = "S";
  public static final String FAILURE = "F";
  public static final String RUNNING = "I";

  // Source File Types
  public static final String MASTER = "Master";
  public static final String PARAMETER = "Parameter";
  public static final String TRANSACTION = "Transaction";
  public static final String ENRICHED = "Enriched";
  public static final String TECHRECON = "Tech Recon";
  public static final String LOG = "Logs";
  public static final String INTERFACE = "Interface";
  public static final String CTRL_FILE = "Control file";
  public static final String MAPPING = "Mapping";
  public static final String REFERENCE = "Reference";
  public static final String WORK = "Work";
  public static final List<String> VALID_SRC_FILE_TYPES = Collections.unmodifiableList(
          Arrays.asList(MASTER, PARAMETER, TRANSACTION, ENRICHED, TECHRECON, LOG, INTERFACE,
                  CTRL_FILE, MAPPING, REFERENCE, WORK));

  // Source File Formats
  public static final String DOUBLE_PIPE_DELIMITER = "||";

  // Load Strategy
  public static final String FULL_LOAD = "Full";
  public static final String APPEND_LOAD = "Delta";
  public static final String HISTORY_LOAD = "History";
  public static final String ADDITIONAL_LOAD = "Additional";
  public static final List<String> VALID_LOAD_STRGY = Collections.unmodifiableList(
          Arrays.asList(FULL_LOAD, APPEND_LOAD, HISTORY_LOAD,ADDITIONAL_LOAD));

  // Decimal Indicators
  public static final String EXPLICIT = "Explicit";
  public static final String IMPLICIT = "Implicit";
  public static final List<String> VALID_DEC_IND = Collections.unmodifiableList(
          Arrays.asList(EXPLICIT, IMPLICIT));
  public static final String IMPLICIT_RULE_PATTERN = "Implicit to Explicit : Precision ";
  public static final String DATE_RULE_PATTERN = "Date Conversion for format ";

  // Source Data Type Constants
  public static final String SRC_ALPHANUMERIC = "A";
  public static final String SRC_SIGNED_DECIMAL = "S";
  public static final String SRC_NUMERIC = "N";
  public static final String SRC_DATE = "D";
  public static final String SRC_TIMESTAMP = "T";
  public static final String SRC_OPEN = "O";
  public static final String SRC_PACKED = "P";
  public static final String SRC_FILE_REFERENCE = "F";

  public static final String STRING = "string";
  public static final String VARCHAR = "varchar";
  public static final String INT = "int";
  public static final String BOOLEAN = "boolean";
  public static final String DOUBLE = "double";
  public static final String DECIMAL = "decimal";
  public static final String DATE = "date";
  public static final String TIMESTAMP = "timestamp";

  public static final List<String> VALID_SRC_DATATYPES = Collections.unmodifiableList(
          Arrays.asList(SRC_ALPHANUMERIC, SRC_SIGNED_DECIMAL, SRC_NUMERIC,
                  SRC_DATE, SRC_TIMESTAMP, SRC_OPEN, SRC_PACKED, SRC_FILE_REFERENCE));

  // Hadoop Compression Codecs
  public static final String SNAPPY = "Snappy";
  public static final String GZIP = "Gzip";
  public static final String BZIP2 = "Bzip2";
  public static final String LZO = "Lzo";
  public static final String LZ4 = "Lz4";
  public static final String ZLIB = "Zlib";
  public static final String SNAPPY_CODEC = "org.apache.hadoop.io.compress.SnappyCodec";
  public static final String GZIP_CODEC = "org.apache.hadoop.io.compress.GzipCodec";
  public static final String BZIP_CODEC = "org.apache.hadoop.io.compress.BZip2Codec";
  public static final String LZO_CODEC = "com.hadoop.compression.lzo.LzoCodec";
  public static final String LZ4_CODEC = "org.apache.hadoop.io.compress.Lz4Codec";
  public static final String ZLIB_CODEC = "org.apache.hadoop.io.compress.DefaultCodec";

  // Framework Configuration File
  public static final String FRAMEWORK_CONF_FILE = "framework-conf.properties";
  public static final String DATABASE_CONF_FILE = "database.properties";
  public static final String DATEFORMAT_CONF_FILE= "dateformat.properties";
  public static final String PRE_PROC_CONF_FILE= "preprocessing-client.properties";
  public static final String STEP_CONF_YAML_FILE= "step-replacer.yaml";
  public static final String INFA_PWD = "Infa.Password";
  public static final String IGNORE_DATE_FORMATS = "IGNORE_DATE_FORMAT_LIST";
  public static final String FORCE_FILE_NAMES = "filesToForce";
  public static final String ENVIRONMENT = "ENVIRONMENT";
  public static final String ENVIRONMENT_NUM = "ENVIRONMENT_NUM";
  public static final String ENVIRONMENT_STR_PARAM = "[ENV]";
  public static final String DATADIR_STR_PARAM = "[DATADIR]";
  public static final String DATADIR = "DATADIR";
  public static final String BDM_PROJ_NAME = "BDM_PROJ_NAME";
  public static final String LANDING_AREA_DIR_PATH = "LANDING_AREA_DIR_PATH";
  public static final String LANDING_AREA_ARCHIVE_DIR_PATH = "LANDING_AREA_ARCHIVE_DIR_PATH";
  public static final String LANDING_AREA_PROCESSING_DIR_PATH = "LANDING_AREA_PROCESSING_DIR_PATH";
  public static final String LANDING_AREA_PSWEBLOG_PROCESSING_DIR_PATH = "LANDING_AREA_PSWEBLOG_PROCESSING_DIR_PATH";
  public static final String DEFAULT_CHARSET = "DEFAULT_CHARSET";
  public static final String DEFAULT_ENCODING = "DEFAULT_ENCODING";
  public static final String TIER_1_HDFS_PATH = "TIER_1_HDFS_PATH";
  public static final String TIER_1_DUMP_HDFS_PATH = "TIER_1_DUMP_HDFS_PATH";
  public static final String TIER_1_HIVE_DB_NM = "TIER_1_HIVE_DB_NM";
  public static final String TIER_1_HIVE_TBL_NM = "TIER_1_HIVE_TBL_NM";
  public static final String TIER_1_HIVE_PARTITION = "TIER_1_HIVE_PARTITION";
  public static final String TIER_1_HIVE_LOCATION = "TIER_1_HIVE_LOCATION";
  public static final String TIER_1_HIVE_DUMP_LOCATION = "TIER_1_HIVE_DUMP_LOCATION";
  public static final String DDS_HIVE_DB_NM = "DDS_HIVE_DB_NM";
  public static final String DDS_HIVE_TBL_NM = "DDS_HIVE_TBL_NM";
  public static final String DDS_HIVE_DUMP_TBL_NM_SUFFIX = "DDS_HIVE_DUMP_TBL_NM_SUFFIX";
  public static final String DDS_HIVE_PARTITION = "DDS_HIVE_PARTITION";
  public static final String DDS_HIVE_PARTITION_WITH_HISTORY = "DDS_HIVE_PARTITION.HISTORY";
  public static final String DDS_HIVE_PARTITION_DROP_ON_RECONCILIATION_FAILURE = "DDS_HIVE_PARTITION_DROP_ON_RECONCILIATION_FAILURE";
  public static final String HIVE_ERR_TBL_NM = "TIER_1_HIVE_ERR_TBL_NM";
  public static final String HIVE_ERR_PARTITION = "TIER_1_HIVE_ERR_PARTITION";
  public static final String HIVE_RUN_STATS = "RUN_HIVE_STATS";
  public static final String IMPALA_RUN_REFRESH = "IMPALA_RUN_REFRESH";
  public static final String VALIDATION_RULE_NUMS = "VALIDATION_RULE_NUMS";
  public static final String SITE_ID = "site_id";
  public static final String COUNTRY_PARAM = "<ctry_cd>";
  public static final String SITE_ID_PARAM = "<site_id>";
  public static final String PROC_INSTANCE_ID_PARAM = "<proc_instance_id>";
  public static final String SRC_SYS_PARAM = "<source_system>";
  public static final String ENV_PARAM = "<environment>";
  public static final String ENV_NUM_PARAM = "<environment#>";
  public static final String SEQUENCE_PARAM = "<sequence>";
  public static final String FREQ_PARAM = "<frequency>";
  public static final String FILENM_PARAM = "<filename>";
  public static final String DELIMIT_CONV_PARAM = "<delimitConvFlag>";
  public static final String FIXED_WIDTHS_PARAM = "<fixedWidths>";
  public static final String FIELDNM_PARAM = "<field_nm>";
  public static final String DBNM_PARAM = "<db_name>";
  public static final String TBLNM_PARAM = "<table_name>";
  public static final String NODE_KEY_PARAM = "<node_key>";
  public static final String NODE_USER_PARAM = "<node_user>";
  public static final String NODE_HOST_PARAM = "<node_host>";
  public static final String CHARSET_PARAM = "<charset>";
  public static final String BIZ_DATE_PARAM = "<biz_dt>";
  public static final String SYS_TS_PARAM = "<timestamp>";
  public static final String QUERY_TYPE_PARAM = "<query_type>";
  public static final String DATE_PARAM = "<date>";
  public static final String PARAM_FILE_PARAM = "<param_file>";
  public static final String INFA_PWD_PARAM = "<infa_pwd>";
  public static final String PROC_INSTANCE_ID = "proc_instance_id";
  public static final String PROC_TIME = "proc_ts";
  public static final String BIZ_DT = "biz_dt";
  public static final String BIZ_DT_EXPR = "biz_dt_expr";
  public static final String RULE_ID = "rule_id";
  public static final String RULE_DESC_TXT = "rule_desc";
  public static final String SQL_FILE_LOC = "SQL_FILE_LOCATION";
  public static final String SQL_FILE_ARCHIVE_LOCATION = "SQL_FILE_ARCHIVE_LOCATION";
  public static final String SQL_FILE_NM = "SQL_FILE_NAME";
  public static final String EXPORT_SQL_FILE_NAME = "EXPORT_SQL_FILE_NAME";
  public static final String SQL_FILE_ARCHIVE_NAME = "SQL_FILE_ARCHIVE_NAME";

  public static final String COUNTRY_PARAM_CD = "CTRY_CD";
  public static final String SITE_ID_PARAM_CD = "SITE_ID";
  public static final String SRC_SYS_PARAM_CD = "SOURCE_SYSTEM";
  public static final String SYS_TS_PARAM_CD = "TIMESTAMP";
  public static final String BIZ_DATE_PARAM_CD = "BIZ_DT";
  public static final String PROC_INSTANCE_ID_PARAM_CD = "PROC_INSTANCE_ID";
  public static final List<String> VALID_PARTITION_PARAMS = Collections.unmodifiableList(
          Arrays.asList(COUNTRY_PARAM_CD, SRC_SYS_PARAM_CD, SITE_ID_PARAM_CD,
                  BIZ_DATE_PARAM_CD, SYS_TS_PARAM_CD, PROC_INSTANCE_ID_PARAM_CD));

  // System Constants
  public static final String SYS_USER_NAME = System.getProperty("user.name");
  public static final String LINE_SEPARATOR = System.getProperty("line.separator", "\r\n");

  // Error Codes
  public static final String VALIDATION_FAILURE = "UOB01"; // Validation Failure
  public static final String SYSTEM_ERROR = "UOB99"; // System Error
  public static final String INVALID_PROCESS_STATUS = "UOB02"; // Invalid Process
  public static final String INVALID_PROCESS = "UOB03"; // Invalid Process
  public static final String INVALID_COUNTRY = "UOB04"; // Invalid Country
  public static final String INVALID_BIZ_DT = "UOB05"; // Invalid Biz Date
  public static final String INVALID_FILE = "UOB06"; // Invalid File
  public static final String BDM_PROC_FAILURE = "UOB07"; // BDM Failure
  public static final String TPT_PROC_FAILURE = "UOB08"; // BDM Failure

  // Line Numbers
  public static final String FIRST = "FIRST";
  public static final String LAST = "LAST";

  // Pre scripts
  public static final String CMD_REMOVE_CTRL_CHAR = "REMOVE_CTRL_CHAR_CMD";
  public static final String CMD_REMOVE_HDER_FTER = "REMOVE_HEADER_FOOTER";
  public static final String CMD_COPY_FILE = "COPY_FILE";

  public static final String ENABLE_REPLACER_STEP = "ENABLE_REPLACER_STEP";
  public static final String YAML_FILE = "config_yaml";

  // Export Parameters
  public static final String EXP_TMP_PARAM_FILE_PATH = "EXP_TEMP_PARAM_FILE_PATH";
  public static final String EXP_PARAM_FILE_DIR_PATH = "EXP_PARAM_FILE_DIR";
  public static final String EXP_PARAM_FILE_NM_PATH = "EXP_PARAM_FILE_NM";
  public static final String EXP_PARAM_FILE_PATH_ARCHIVE = "EXP_PARAM_FILE_PATH_ARCHIVE";
  public static final String EXP_FILE_PATH = "EXP_FILE_PATH";

  // BDM
  public static final String BDM_PARAM_TEMPLATE = "BDM_PARAM_FILE_TEMPLATE";
  public static final String BDM_PARAM_FILE_PATH = "BDM_PARAM_FILE_PATH";
  public static final String BDM_PARAM_FILE_PATH_ARCHIVE = "BDM_PARAM_FILE_PATH_ARCHIVE";
  public static final String BDM_CMD = "BDM_CMD";
  public static final String BDM_T11_MAPPING_NAME = "BDM_T11_MAPPING_NAME";
  public static final String BDM_T11_CMD = "BDM_T11_CMD";
  public static final String BDM_RUN_T1_CMD = "BDM_RUN_T1_CMD";
  public static final String BDM_PROCESS_EMPTY_FILE = "BDM_PROCESS_EMPTY_FILE";
  public static final String T1_TGT_FILE_DIR = "TGT_FILE_DIR";
  public static final String T1_TGT_FILE = "TGT_FILE";
  public static final String T1_SRC_FILE_DIR = "SRC_FILE_DIR";
  public static final String T1_SRC_FILE = "SRC_FILE";
  public static final String STG_DB = "STG_DB";
  public static final String STG_TBL = "STG_TBL";
  public static final String TGT_DB = "TGT_DB";
  public static final String TGT_TBL = "TGT_TBL";
  public static final String TGT_ERR_TBL = "TGT_ERR_TBL";
  public static final String TGT_PROC_ID = "TGT_PROC_ID";
  public static final String TGT_BIZ_DT = "TGT_BIZ_DT";
  public static final String TGT_CTRY_CD = "TGT_CTRY_CD";
  public static final String TGT_CNX_NM = "TGT_CNX_NM";
  public static final String HDFS_CNX_NM = "HDFS_CNX_NM";
  public static final String HIVE_CNX_NM = "HIVE_CNX_NM";
  public static final String VALIDATION = "VALIDATION";
  public static final String[] TGT_RULES = new String[62];

  // Stages
  // File Ingestion
  public static final String STAGE_INGEST_PROC_INIT = "211";
  public static final String STAGE_INGEST_T1_BDM_EXEC = "212";
  public static final String STAGE_INGEST_T11_BDM_EXEC = "213";
  public static final String STAGE_INGEST_PROC_FINAL = "214";

  // T1.4 Process
  public static final String STAGE_REGISTER_PROC_INIT = "331";
  public static final String STAGE_REGISTER_PROC_HIVE_TABLE = "332";
  public static final String STAGE_REGISTER_PROC_TDCH_INSERT = "333";
  public static final String STAGE_REGISTER_PROC_TECH_RECON = "334";

  // File Export
  public static final String STAGE_EXPORT_PROC_INIT = "711";
  public static final String STAGE_EXPORT_TPT_EXEC = "712";
  public static final String STAGE_EXPORT_PROC_FINAL = "713";

  // SQL File Generation
  public static final String METADATA_INSERT = "MI";
  public static final String METADATA_DELETE = "MD";
  public static final String HIVE_CREATE = "HC";
  public static final String HIVE_RENAME = "HR";

  // Export Process
  public static final String TPT_EXEC_COMMAND = "TPT_EXPORT_COMMAND";

  // TDCH
  public static final String TDCH_JAR = "TDCH_JAR";
  public static final String TDCH_LIB_JARS = "TDCH_LIB_JARS";
  public static final String TDCH_USE_TDWALLET = "TDCH_USE_TDWALLET";

  // T1.4
  public static final String T14_FILE_FORMAT = "T14_FILE_FORMAT";
  public static final String T14_ALLOW_INGESTION_BYPASS = "T14_ALLOW_INGESTION_BYPASS";
  public static final String T14_SKIP_HASHSUM_RECONCILIATION = "T14_SKIP_HASHSUM_RECONCILIATION";

  // EDF-203
  public static final String SOURCEFILENAME= "sourceFileName";
  public static final String PROCESSID = "processID";
  public static final String COUNTRYCODE = "countryCode";
  public static final String BUSINESSDATE = "businessDate";
  public static final String SOURCESYSTEMCODE ="sourceSystemCode";

  public static final String[] SOURCE_FIELD_RULES = new String[11];
  public static final String HDFS_REFERENCE_FOLDER = "HDFS_TARGET_FOLDER";

  // EDF-214
  public static final String PERSONETICS_ZIPFILE_REGEX = "personetics.zipFile_Regx";
  public static final String PERSONETICS_PREFIX_REGX = "personetics.newlineregex";
  public static final String PERSONETICS_DEL_PROCESS_FILE = "personetics.deleteProcessFiles";

  //Adobe Site Catalyst Master Process
  public static final String ADOBE_SITE_CATALYST_MASTER_PROCESS = "FI_ADB_MST_PROCESS_D01";
  public static final String ADOBE_SITE_CATALYST_MASTER_PROCESS_HISTORY = "FI_ADB_MST_PROCESS_H01";
  public static final String ADOBE_SITE_CATALYST_SRC_SYS_CD = "ADB";
  public static final String ADOBE_SITE_CATALYST_HIT_DATA_PROC = "FI_ADB_hit_data_D01";
  public static final String ADOBE_SITE_CATALYST_HIT_DATA_PROC_HISTORY = "FI_ADB_hit_data_H01";
  public static final String ADOBE_SITE_CATALYST_FILE_EXTN = "tsv";
  public static final String ADOBE_SITE_CATALYST_PROC_TYP_CD = "21";
  public static final String ADOBE_SITE_CATALYST_PROC_GRP_ID = "126";
  public static final String ADOBE_SITE_CATALYST_DEPL_NODE_NM = "Node1";
  public static final String ADOBE_SITE_CATALYST_PROC_CRIT_CD = "M";
  public static final String ADOBE_SITE_CATALYST_IS_ACT_FLG = "Y";

  //Excel Streaming
  public static final String EXCEL_STREAM_ROW_CATCH_SIZE = "EXCEL_STREAM_ROW_CATCH_SIZE";
  public static final String EXCEL_STREAM_BUFFER_SIZE = "EXCEL_STREAM_BUFFER_SIZE";
  public static final String EXCEL_STREAM_ENABLE_FLAG = "EXCEL_STREAM_ENABLE_FLAG";
  
  // Validation Error Messages
  public static final String BIZ_DATE_VALIATION_MESSAGE = "Suppression of Business Date Validation is enabled";
  public static final String ROW_COUNT_RECONCILIATION_MESSAGE = "Suppression of Row Count Reconciliation is enabled";
  public static final String ROW_COUNT_VALIDATION_MESSAGE = "Suppression of Row Count Validation is enabled";
  public static final String HASH_SUM_RECONCILIATION_MESSAGE = "Suppression of Hash Sum Reconciliation is enabled";
  public static final String MD5_SUM_VALIATION_MESSAGE = "Suppression of MD5 Sum Validation is enabled";
  public static final String SKIP_ERR_RECORDS_MESSAGE = "Suppression of Error Records is enabled";
  
  // LOB Stamping Registration Constants
  public static final String LOB_REGISTRATION_FILE_NAME = "lob_stamping_registration";
  
  //Spark Ingestion
  public static final String SPARK_HOME = "SPARK_HOME";
  public static final String SPARK_MASTER = "SPARK_MASTER";
  public static final String SPARK_DEPLOY_MODE = "SPARK_DEPLOY_MODE";
  public static final String KEYTAB_LOCATION = "KEYTAB_LOCATION";
  public static final String KEYTAB_NAME = "KEYTAB_NAME";
  public static final String JAAS_CONFIG_LOCATION = "JAAS_CONFIG_LOCATION";
  public static final String SPARK_DEPLOY_MODE_ACCEPTED = "cluster";
  public static final String ADDITIONAL_JARS_CONFIG_NAME = "ADDITIONAL_JARS_CONFIG_NAME";
  public static final String APPLICATION_JAR_PATH = "APPLICATION_JAR_PATH";
  public static final String APPLICATION_MAIN_CLASS_NAME = "APPLICATION_MAIN_CLASS_NAME";
  public static final String APPLICATION_PROPERTY_FILE_LOCATION = "APPLICATION_PROPERTY_FILE_LOCATION";
  public static final String SPARK_DRIVER_MEMORY = "SPARK_DRIVER_MEMORY";
  public static final String SPARK_EXECUTOR_MEMORY = "SPARK_EXECUTOR_MEMORY";
  public static final String SPARK_EXECUTOR_INSTANCE = "SPARK_EXECUTOR_INSTANCE";
  public static final String SPARK_EXECUTOR_CORES = "SPARK_EXECUTOR_CORES";
  
  public static final String PARALLEL_PRE_PROCESSING_PARAM_NM="PARALLEL_PRE_PROCESSING";
  public static final String SPARK_INGESTION_PARAM_NM="SPARK_INGESTION";

  /* Constants for Spark Execution Params registration for Bypass Informatica and Parallel Pre-Processing */
  public static final String SPARK_EXEC_PARAMS_SHEET = "SparkExecutionParams";
  public static final String SPARK_BASED_INGESTION_SHEET = "SparkBasedIngestion";
  public static final String PRE_PROCESS_FLAG_SHEET = "PreProcessFlag";
  public static final String PRE_PROCESS_PROC_PARAM_SHEET = "PreProcessProcParam";
  public static final String SPARK_EXEC_PARAMS_MI_FILE = "DL_FI_SPARK_EXEC_PARAMS_MI.sql";
  public static final String SPARK_EXEC_PARAMS_MD_FILE = "DL_FI_SPARK_EXEC_PARAMS_MD.sql";
  public static final String SPARK_BASED_INGESTION_MI_FILE = "DL_FI_SPARK_BASED_INGESTION_MI.sql";
  public static final String SPARK_BASED_INGESTION_MD_FILE = "DL_FI_SPARK_BASED_INGESTION_MD.sql";
  public static final String PRE_PROCESS_FLAG_MI_FILE = "DL_FI_PRE_PROCESS_FLAG_MI.sql";
  public static final String PRE_PROCESS_FLAG_MD_FILE = "DL_FI_PRE_PROCESS_FLAG_MD.sql";
  public static final String PRE_PROCESS_PROC_PARAM_MI_FILE = "DL_FI_PRE_PROCESS_PROC_PARAM_MI.sql";
  public static final String PRE_PROCESS_PROC_PARAM_MD_FILE = "DL_FI_PRE_PROCESS_PROC_PARAM_MD.sql";

  	public static final String UNS_HDFS_INPUT_PATH = "UNS_HDFS_INPUT_PATH";
	public static final String UNS_INVOKE_CLASS_NAME = "UNS_INVOKE_CLASS_NAME";
	public static final String UNS_DOCUMENT_COL_NAME = "UNS_DOCUMENT_COL_NAME";
	public static final String UNS_COLLECTON_REGX_MAP = "UNS_COLLECTON_REGX_MAP";
	public static final String PROD_INSTANCE_ID = "PROD_INSTANCE_ID";
	public static final String UNS_HIVE_ROW_COUNT = "UNS_HIVE_ROW_COUNT";
	public static final String HDFS_HOST_NAME = "HDFS_HOST_NAME";
	public static final String UNS_PROCESS_RESULT = "UNS_PROCESS_RESULT";
	public static final String UNS_FAILED = "UNS_FAILED";
	public static final String UNS_PROCESS_HIVE_SQL = "UNS_PROCESS_HIVE_SQL";
	public static final String UNS_EXCEPTION_RESULT = "UNS_EXCEPTION_RESULT";
	public static final String SRC_SYS_CD = "SRC_SYS_CD";
	
	
	public static final String HDFSINPUTPATH = "HDFSINPUTPATH";
	public static final String INPUTLIST = "INPUTLIST";
	public static final String REGEXLISTS = "REGEXLISTS";
	public static final String PROCINSTANCEID = "PROCINSTANCEID";
	public static final String HIVEROWCOUNT = "HIVEROWCOUNT";
	public static final String IMPALA = "IMPALA";
	public static final String BIZ_DATE_STRING = "<biz_dt>";
	public static final String COUNTRY_CODE_STRING = "<ctry_cd>";
	
	
	public static final String OSM_MERGE_FILE_FLAG = "OSM_MERGE_FILE_FLAG";
	

	public static final List<String> UNS_FILE_TYPES = new ArrayList<String>();
	
	static {
		UNS_FILE_TYPES.add("PDF");
		UNS_FILE_TYPES.add("DOCX");
		UNS_FILE_TYPES.add("XLSX");
		UNS_FILE_TYPES.add("DOC");
		UNS_FILE_TYPES.add("XLS");
		UNS_FILE_TYPES.add("MSG");
		UNS_FILE_TYPES.add("EML");
		UNS_FILE_TYPES.add("XML");
		UNS_FILE_TYPES.add("JSON");
	}
  

  static {
    for (int i = 0; i < 11; i++) {
      SOURCE_FIELD_RULES[i] = "SOURCE_FILE_FIELD_NAME_" + Integer.toString(i);
    }
  }


  static {
    for (int i = 0; i <= 61; i++) {
      TGT_RULES[i] = "TGT_RULE_" + Integer.toString(i);
    }
  }

  public enum FileLayoutIdentifier {

    FIXED("FXD"),
    DELIMITED("DEL"),
    FIXED_TO_DELIMITED("FXD2DEL"),
    XLS_WITH_HEADER("XlsWHeader"),
    XLS_WITHOUT_HEADER("XlsWOHeader"),
    XLSX_WITH_HEADER("XlsxWHeader"),
    XLSX_WITHOUT_HEADER("XlsxWOHeader"),
    REG_EXPRESSION("REGX"),
    CSV("CSV"), // SM: for SWIFT file
    NONE("");

    String delimiterType;

    FileLayoutIdentifier(String delimiterType) {
      this.delimiterType = delimiterType;
    }

    public String getDelimiterType() {
      return this.delimiterType;
    }

    public static FileLayoutIdentifier getFileLayoutType(String delimiterType) {
      FileLayoutIdentifier identifier = NONE;
      for (FileLayoutIdentifier fileIndentifier : FileLayoutIdentifier.values()) {
        if (fileIndentifier.getDelimiterType().equalsIgnoreCase(delimiterType)) {
          identifier = fileIndentifier;
          break;
        }
      }
      return identifier;
    }
  }

  public enum DateConverter {

    FORMAT_1("YYYYMMDD", "yyyyMMdd"),
    FORMAT_2("YYYYMMDDHHMMSS", "yyyyMMddHHmmss"),
    FORMAT_3("YYYY-MM-DD", "yyyy-MM-dd"),
    FORMAT_4("YYYYMMDDHHMI", "yyyyMMddHHmm"),
    FORMAT_5("YYYY-MM-DD__HH||MI||SS", "yyyyMMdd HH:mm:ss"),
    FORMAT_6("CCYY-MM-DD", "yyyy-MM-dd"),
    FORMAT_7("24HHMISS", "HHmmss"),
    FORMAT_8("DDMMYY", "ddMMyy"),
    FORMAT_9("HHMMSS", "HHmmss"),
    FORMAT_10("MON__YYYY", "MMM yyyy"),
    FORMAT_11("MMM__YYYY", "MMM yyyy"),
    FORMAT_12("YYYY", "yyyy"),
    FORMAT_13("YYYY-MM-DD-HH.MM.SS.NNNNNN", "yyyy-MM-dd-HH.mm.ss.SSSSSS"),
    FORMAT_14("DDMMYYYY", "ddMMyyyy"),
    FORMAT_15("YYYYDDD", "yyyyDDD"),
    FORMAT_16("DDDYYYY", "DDDyyyy"),
    FORMAT_17("DD/MM/YY", "dd/MM/yy"),
    FORMAT_18("YYYY-MM-DD-HH.MM.SS.mmmmmm", "yyyy-MM-dd-HH.mm.ss.SSSSSS"),
    FORMAT_19("00YYYYMMDD", "'00'yyyyMMdd"),
    FORMAT_20("DD/MM/YYYY HH:MI", "dd/MM/yyyy HH:mm"),
    FORMAT_21("DD/MM/YYYY", "dd/MM/yyyy"),
    FORMAT_22("DD/MM/YYYY HH:MI:SS", "dd/MM/yyyy HH:mm:ss"),
    NONE("", ""),;

    String specFormat;
    String javaFormat;

    DateConverter(String specFormat, String javaFormat) {
      this.specFormat = specFormat;
      this.javaFormat = javaFormat;
    }

    public String getSpecFormat() {
      return this.specFormat;
    }

    public String getJavaFormat() {
      return this.javaFormat;
    }

    public static String getEquivalentJavaFormat(String specFormat) {
      DateConverter df = NONE;
      for (DateConverter dateFormat : DateConverter.values()) {
        if (dateFormat.getSpecFormat().equals(specFormat)) {
          df = dateFormat;
          break;
        }
      }
      return df.getJavaFormat();
    }
  }

}
