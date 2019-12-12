package com.uob.edag.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.HiveConnectionFactory;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.HiveMapper;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FieldModel.RecordType;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.utils.PropertyLoader;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/28/2016
 * @Description : The file is used for performing Database operations on Hive.
 * 
 */

public class HiveGenerationDao extends AbstractDao {
	
	public static final String DUMP_FIELD_NAME_PROPERTY = "DDS_HIVE_DUMP_FIELD_NM";
	
  public static final PredefinedException CANNOT_GET_CREATE_DB_HQL = new PredefinedException(HiveGenerationDao.class, "CANNOT_GET_CREATE_DB_HQL", "Unable to get create database statement for schema {0}: {1}");
	public static final PredefinedException CANNOT_GET_RENAME_TABLE_HQL = new PredefinedException(HiveGenerationDao.class, "CANNOT_GET_RENAME_TABLE_HQL", "Unable to get statement to rename {0} to {1}: {2}");
	
	private String dumpFieldName;

	public HiveGenerationDao() {
		super(HiveConnectionFactory.getFactory());
		
		dumpFieldName = PropertyLoader.getProperty(DUMP_FIELD_NAME_PROPERTY);
	}

	/**
   * This method will generate the SQL to create a database on Hive.
   * @param schemaName the Name of the schema to be created
   * @return the create Database SQL statement
   * @throws Exception when there is an error generating the SQL
   */
  public String createDatabase(String schemaName) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
      MappedStatement ms = session.getConfiguration().getMappedStatement(HiveMapper.class.getName() + ".createDatabase");
      BoundSql boundSql = ms.getBoundSql(schemaName);
      String sql = boundSql.getSql() + ";";
      logger.debug("Create database SQL for " + schemaName + " is " + sql);
      return sql;
    } catch (PersistenceException e) {
    	throw new EDAGMyBatisException(HiveGenerationDao.CANNOT_GET_CREATE_DB_HQL, schemaName, e.getMessage());
    }
  }

  /**
   * This method will generate the SQL to create the error table in staging database on Hive.
   * @param procModel The Process model object containing the metadata
   * @param country The Country for which the Table is being created.
   * @return the SQL statement for the Create Table command
   * @throws Exception when there is an error generating the SQL.
   */
  public String createErrorHiveTable(ProcessModel procModel, String country) {
  	HadoopModel hadoopModel = procModel.getDestInfo();

    String stagingDbName = hadoopModel.getStagingDbName();
    String newStagingDbName = stagingDbName.replace(UobConstants.COUNTRY_PARAM, country);

    // Table Creation
    StringBuilder createTableSql = new StringBuilder();
    createTableSql.append("CREATE TABLE IF NOT EXISTS " + newStagingDbName + "." + hadoopModel.getStagingTableName() + 
    		                  UobConstants.UNDERSCORE + "err" + " (");

    // Field Definition
    Map<Integer, FieldModel> fieldsInfo = hadoopModel.getDestFieldInfo();
    int size = fieldsInfo.size();
    for (int j = 1; j <= size; j++) {
      FieldModel field = fieldsInfo.get(j);
      if (field.getRecordType().isData()) {
        createTableSql.append("`" + field.getNormalizedFieldName() + "`");
        createTableSql.append(" STRING COMMENT '");
        createTableSql.append(field.getNormalizedFieldDesc());
        createTableSql.append("'");
        createTableSql.append(UobConstants.COMMA);
      }
    }
    
    if (createTableSql.length() > 0) {
      createTableSql.deleteCharAt(createTableSql.length() - 1);
    }
    createTableSql.append(")");

    createTableSql.append(" COMMENT '");
    createTableSql.append(procModel.getNormalizedProcDesc());
    createTableSql.append("'");

    // Partitioning Info
    createTableSql.append(" PARTITIONED BY (");
    String hivePartition = hadoopModel.getStagingHivePartition();
    String[] hivePartitionList = hivePartition.split(UobConstants.COMMA);
    int partitionSize = hivePartitionList.length;
    int num = 1;
    for (String key : hivePartitionList) {
      String partitionKey = key.substring(0, key.indexOf(UobConstants.EQUAL));
      createTableSql.append(partitionKey);
      createTableSql.append(" STRING");
      if (num++ != partitionSize) {
        createTableSql.append(UobConstants.COMMA);
      }
    }
    
    createTableSql.append(")");
    createTableSql.append(" STORED AS TEXTFILE ;");

    String sql = createTableSql.toString(); 
    logger.debug("Error Hive table for " + procModel.getProcId() + ", country " + country + ": " + sql);
    return sql;
  }

  /**
   * This method is used to generate the SQL to rename a hive table.
   * @param schemaName The name of the schema
   * @param tableName The name of the hive table
   * @param newTableName The new name of the hive table to be renamed to.
   * @return the SQL statement for the Rename Hive command.
   * @throws Exception when there is an error generating the SQL.
   */
  public String renameHiveTable(String schemaName, String tableName, String newTableName) throws EDAGMyBatisException {
  	String fullTableName = schemaName + "." + tableName;
    String fullNewTableName = schemaName + "." + newTableName;
    try (SqlSession session = openSession(true)) {
      Map<String, String> params = new HashMap<String, String>();
      params.put("fullTableName", fullTableName);
      params.put("fullNewTableName", fullNewTableName);

      MappedStatement ms = session.getConfiguration().getMappedStatement(HiveMapper.class.getName() + ".renameHiveTable");
      BoundSql boundSql = ms.getBoundSql(params);
      String sql = boundSql.getSql() + ";";
      logger.debug("Hive table rename statement for " + params + ": " + sql);
      return sql;
    } catch (PersistenceException e) {
        throw new EDAGMyBatisException(HiveGenerationDao.CANNOT_GET_RENAME_TABLE_HQL, fullTableName, fullNewTableName, e.getMessage());
    }
  }
  
  /**
   * This method is used to generate the SQL to create the hive table in the staging area.
   * @param procModel The Process Model containing the metadata for the table creation.
   * @param country The country for which the table is to be created.
   * @return the SQL statatement generated for the create table command.
   * @throws Exception when there is an error generating the SQL statement.
   */
  protected String createStagingHiveTable(String stagingDbName, String stagingTableName, 
  		                                    Map<Integer, FieldModel> fieldsInfo, String stagingHivePartition, 
  		                                    String stagingHiveLocation,
  		                                    String fileFormat, String columnDelimiter, String textDelimiter, 
  		                                    String serialization, String processDescription, String country) {
  	
    String newStagingDbName = stagingDbName.replace(UobConstants.COUNTRY_PARAM, country);

    // Table Creation
    StringBuilder createTableSql = new StringBuilder();
    createTableSql.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + newStagingDbName + "." + 
                          stagingTableName + " (");

    // Field Definition
    int size = fieldsInfo.size();
    for (int j = 1; j <= size; j++) {
      FieldModel field = fieldsInfo.get(j);
      if (field.getRecordType().isData()) {
        createTableSql.append("`" + field.getNormalizedFieldName() + "`");
        createTableSql.append(" STRING COMMENT '");
        createTableSql.append(field.getNormalizedFieldDesc());
        createTableSql.append("'");
        createTableSql.append(UobConstants.COMMA);
      }
    }
    
    if (createTableSql.length() > 0) {
      createTableSql.deleteCharAt(createTableSql.length() - 1);
    }
    createTableSql.append(")");

    createTableSql.append(" COMMENT '");
    createTableSql.append(processDescription);
    createTableSql.append("'");

    // Partitioning Info
    createTableSql.append(" PARTITIONED BY (");
    String[] hivePartitionList = stagingHivePartition.split(UobConstants.COMMA);
    int partitionSize = hivePartitionList.length;
    int num = 1;
    for (String key : hivePartitionList) {
      String partitionKey = key.substring(0, key.indexOf(UobConstants.EQUAL));
      createTableSql.append(partitionKey);
      createTableSql.append(" STRING");
      if (num++ != partitionSize) {
        createTableSql.append(UobConstants.COMMA);
      }
    }
    createTableSql.append(")");

    // File Layout Definition
    if (UobConstants.FIXED_FILE.equalsIgnoreCase(fileFormat)) {
      createTableSql.append(" ROW FORMAT SERDE 'com.charles.hive.serde.regex.RegexEncodingAwareSerDe'" + 
                            " WITH SERDEPROPERTIES (\"input.regex\" = \"");
      for (int j = 1; j <= size; j++) {
        FieldModel field = fieldsInfo.get(j);
        int colLen = field.getLength();
        createTableSql.append("(.{");
        createTableSql.append(colLen);
        createTableSql.append("})");
      }
      
      createTableSql.append("\"," + "\"output.format.string\" = \"");
      int nums = 1;
      for (int j = 1; j <= size; j++) {
        createTableSql.append("%");
        createTableSql.append(j);
        createTableSql.append("$s");
        if (nums++ < fieldsInfo.size()) {
          createTableSql.append(UobConstants.SPACE);
        }
      }
      
      createTableSql.append("\"");
      if (serialization != null) {
        createTableSql.append(",");
        createTableSql.append("\"serialization.encoding\" = \"");
        createTableSql.append(serialization);
        createTableSql.append("\"");
      }
      createTableSql.append(")");
    } else if (UobConstants.REG_EXPRESSION.equalsIgnoreCase(fileFormat) || UobConstants.CSV.equalsIgnoreCase(fileFormat)) {
    	
    	createTableSql.append(" ROW FORMAT SERDE ");
        createTableSql.append("'org.apache.hadoop.hive.serde2.OpenCSVSerde'");
        createTableSql.append(" WITH SERDEPROPERTIES ( ");
        createTableSql.append("'field.delim'='" + columnDelimiter + "',"); 
        createTableSql.append("'line.delim'='\\n'");
        if (StringUtils.isNotBlank(textDelimiter)) {
          createTableSql.append(",");
          createTableSql.append("'escape.delim'='");
          createTableSql.append(textDelimiter);
          createTableSql.append("'");
        }

        if (serialization != null) {
          createTableSql.append(",");
          createTableSql.append("'serialization.encoding' = '");
          createTableSql.append(serialization);
          createTableSql.append("'");
        }
        createTableSql.append(")");
    	
    } else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(fileFormat) || 
    		       UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(fileFormat) ||
    		       checkForDifferentSourceFileFormat(fileFormat)) {
      createTableSql.append(" ROW FORMAT SERDE ");
      // Fix with LazySimpleSerDe limitation with multi-characters field delimiter
      // https://thinkbiganalytics.atlassian.net/browse/EDF-76
      // https://issues.apache.org/jira/browse/HIVE-5871
      // https://cwiki.apache.org/confluence/display/Hive/MultiDelimitSerDe
      if (StringEscapeUtils.unescapeJava(columnDelimiter).length() > 1) {
        createTableSql.append("'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'");
      } else {
        createTableSql.append("'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'");
      }
      createTableSql.append(" WITH SERDEPROPERTIES ( ");
      createTableSql.append("'field.delim'='" + columnDelimiter + "',"); 
      createTableSql.append("'line.delim'='\\n'");
      if (StringUtils.isNotBlank(textDelimiter)) {
        createTableSql.append(",");
        createTableSql.append("'escape.delim'='");
        createTableSql.append(textDelimiter);
        createTableSql.append("'");
      }

      if (serialization != null) {
        createTableSql.append(",");
        createTableSql.append("'serialization.encoding' = '");
        createTableSql.append(serialization);
        createTableSql.append("'");
      }
      createTableSql.append(")");
    }

    // Hive Table Location
    String stagingHiveLoc = stagingHiveLocation.replace(UobConstants.COUNTRY_PARAM, country);
    createTableSql.append(" STORED AS TEXTFILE LOCATION '");
    createTableSql.append(stagingHiveLoc);
    createTableSql.append("'");
    // createTableSql.append(";");

    return createTableSql.toString();
  }
  
  public String createDumpStagingHiveTable(ProcessModel procModel, ProcessInstanceModel procInstanceModel) {
  	HadoopModel hadoopModel = procModel.getDestInfo();
  	CountryAttributes countryAttributes = procModel.getCountryAttributesMap().get(procInstanceModel.getCountryCd());
    String serialization = countryAttributes != null ? countryAttributes.getEncoding(true) : PropertyLoader.getProperty(UobConstants.DEFAULT_ENCODING);
    String fileFormat = PropertyLoader.getProperty("com.uob.edag.validation.InterfaceSpecHandler.FileFormat.PQ");
    String columnDelimiter = PropertyLoader.getProperty("com.uob.edag.validation.InterfaceSpecHandler.FieldDelimiter.PQ");
    String textDelimiter = PropertyLoader.getProperty("com.uob.edag.validation.InterfaceSpecHandler.StringEnclosure.PQ");
    
    FieldModel dumpField = new FieldModel();
  	dumpField.setRecordType(RecordType.FIELD_INFO);
  	dumpField.setFieldNum(1);
  	dumpField.setFieldName(dumpFieldName);
  	dumpField.setDataType(UobConstants.SRC_ALPHANUMERIC);
  	dumpField.setFieldDesc("Field to store records dumped from history file");
  	
  	Map<Integer, FieldModel> fieldsInfo = new HashMap<Integer, FieldModel>();
  	fieldsInfo.put(1, dumpField);
  	
  	String sql = createStagingHiveTable(hadoopModel.getStagingDbName(), hadoopModel.getDumpStagingTableName(),
  			                                fieldsInfo, hadoopModel.getStagingHivePartition(),
  			                                hadoopModel.getDumpStagingHiveLocation(procModel, procInstanceModel), fileFormat, columnDelimiter, textDelimiter, 
  			                                serialization, procModel.getNormalizedProcDesc(), procInstanceModel.getCountryCd());
  	logger.debug("Create dump staging Hive table SQL for " + procModel.getProcId() + ", country " + procInstanceModel.getCountryCd() + " is " + sql);
    return sql;
  }
  
  public String createStagingHiveTable(ProcessModel procModel, String country) {
  	HadoopModel hadoopModel = procModel.getDestInfo();
  	FileModel srcModel = procModel.getSrcInfo();
    String fileFormat = StringUtils.trimToEmpty(srcModel.getSourceFileLayoutCd());
    CountryAttributes countryAttributes = procModel.getCountryAttributesMap().get(country);
    String serialization = countryAttributes != null ? countryAttributes.getEncoding(true) : PropertyLoader.getProperty(UobConstants.DEFAULT_ENCODING);
  	
  	String sql = createStagingHiveTable(hadoopModel.getStagingDbName(), hadoopModel.getStagingTableName(), 
  			                                hadoopModel.getDestFieldInfo(), hadoopModel.getStagingHivePartition(),
  			                                hadoopModel.getStagingHiveLocation(), fileFormat, srcModel.getColumnDelimiter(),
  			                                srcModel.getTextDelimiter(), serialization, procModel.getNormalizedProcDesc(), country) + ";";
  	logger.debug("Create staging Hive table SQL for " + procModel.getProcId() + ", country " + country + " is " + sql);
    return sql;
  }

  /**
   * This method is used to generate the SQL to create the hive table in the DDS area.
   * @param procModel The Process Model containing the metadata for the table creation.
   * @return the SQL statatement generated for the create table command.
   * @throws Exception when there is an error generating the SQL statement.
   */
  protected String createHiveTable(String hiveDbName, String hiveTableName, Map<Integer, FieldModel> fieldMap,
  		                             String hivePartition, String formatId, String compressId,
  		                             String processDescription) {
    // Table Creation
    StringBuilder createTableSql = new StringBuilder();
    createTableSql.append("CREATE TABLE IF NOT EXISTS ");
    
    createTableSql.append(hiveDbName);
    createTableSql.append(".");
    createTableSql.append(hiveTableName);
    createTableSql.append(" (");

    // Field Definition
    int size = fieldMap.size();
    for (int j = 1; j <= size; j++) {
      FieldModel field = fieldMap.get(j);
      if (field.getRecordType().isData()) {
        createTableSql.append("`" + field.getNormalizedFieldName() + "`");
        createTableSql.append(" ");
        switch (field.getDataType()) {
          case UobConstants.SRC_ALPHANUMERIC:
          case UobConstants.SRC_FILE_REFERENCE:
            createTableSql.append(UobConstants.STRING);
            break;
          case UobConstants.SRC_SIGNED_DECIMAL:
          case UobConstants.SRC_NUMERIC:
          case UobConstants.SRC_PACKED:
            int length = field.getLength();
            int precision = field.getDecimalPrecision();
            if (length > 38) {
              createTableSql.append(UobConstants.DECIMAL);
              createTableSql.append("(");
              createTableSql.append(38); //TODO
              createTableSql.append(UobConstants.COMMA);
              createTableSql.append(precision);
              createTableSql.append(")");
//          } else if (length + precision > 38) {
//            createTableSql.append(UobConstants.DOUBLE);
            } else {
              createTableSql.append(UobConstants.DECIMAL);
              createTableSql.append("(");
              createTableSql.append(length);
              createTableSql.append(UobConstants.COMMA);
              createTableSql.append(precision);
              createTableSql.append(")");
            }
            break;
          case UobConstants.SRC_DATE:
            createTableSql.append(UobConstants.TIMESTAMP);
            break;
          case UobConstants.SRC_TIMESTAMP:
            createTableSql.append(UobConstants.TIMESTAMP);
            break;
          case UobConstants.SRC_OPEN:
            createTableSql.append(UobConstants.STRING);
            break;
          default:
            break;
        }
        
        createTableSql.append(" COMMENT '");
        createTableSql.append(field.getNormalizedFieldDesc());
        createTableSql.append("'");
        createTableSql.append(UobConstants.COMMA);
      }
    }
    
    if (!hivePartition.toLowerCase().contains(UobConstants.PROC_INSTANCE_ID.toLowerCase())) {
	    createTableSql.append(UobConstants.PROC_INSTANCE_ID);
	    createTableSql.append(UobConstants.SPACE);
	    createTableSql.append(UobConstants.STRING); 
	    createTableSql.append(UobConstants.COMMA);
    }
    
    createTableSql.append(UobConstants.PROC_TIME);
    createTableSql.append(UobConstants.SPACE);
    createTableSql.append(UobConstants.TIMESTAMP);
    createTableSql.append(")");

    createTableSql.append(" COMMENT '");
    createTableSql.append(processDescription);
    createTableSql.append("'");

    // Partitioning Info
    createTableSql.append(" PARTITIONED BY (");
    String[] hivePartitionList = hivePartition.split(UobConstants.COMMA);
    int partitionSize = hivePartitionList.length;
    int num = 1;
    for (String key : hivePartitionList) {
      String partitionKey = key.substring(0, key.indexOf(UobConstants.EQUAL));
      createTableSql.append(partitionKey);
      createTableSql.append(" STRING");
      if (num++ != partitionSize) {
        createTableSql.append(UobConstants.COMMA);
      }
    }
    createTableSql.append(")");

    // Hive Table Location
    createTableSql.append(" STORED AS ");
    switch (formatId) {
      case UobConstants.TEXT_CD:
        createTableSql.append(" TEXTFILE ");
        createTableSql.append(" TBLPROPERTIES(\"compression.type\"=\"BLOCK\"," + 
                              "\"compression.codec\"=\"");
        if (UobConstants.SNAPPY_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.SNAPPY_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.GZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.GZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.BZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.BZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZO_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZO_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZ4_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZ4_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.ZLIB_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.ZLIB_CODEC);
          createTableSql.append("\")");
        }
        break;
      case UobConstants.PARQUET_CD:
        createTableSql.append("PARQUET ");
        createTableSql.append(" TBLPROPERTIES(\"parquet.compress\"=\"");
        if (UobConstants.SNAPPY_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.SNAPPY_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.GZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.GZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.BZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.BZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZO_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZO_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZ4_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZ4_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.ZLIB_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.ZLIB_CODEC);
          createTableSql.append("\")");
        }
        break;
      case UobConstants.ORC_CD:
        createTableSql.append("ORC ");
        createTableSql.append(" TBLPROPERTIES(\"parquet.compress\"=\"");
        if (UobConstants.SNAPPY_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.SNAPPY_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.GZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.GZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.BZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.BZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZO_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZO_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZ4_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZ4_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.ZLIB_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.ZLIB_CODEC);
          createTableSql.append("\")");
        }
        break;
      case UobConstants.SEQ_CD:
        createTableSql.append("SEQUENCEFILE ");
        if (UobConstants.SNAPPY_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.SNAPPY_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.GZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.GZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.BZIP_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.BZIP_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZO_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZO_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.LZ4_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.LZ4_CODEC);
          createTableSql.append("\")");
        } else if (UobConstants.ZLIB_CD.equalsIgnoreCase(compressId.trim())) {
          createTableSql.append(UobConstants.ZLIB_CODEC);
          createTableSql.append("\")");
        }
        break;
      default:
        createTableSql.append("TEXTFILE ");
        break;
    }

    // createTableSql.append(";");

    return createTableSql.toString();
  }
  
  public String createHiveTable(ProcessModel procModel) {
  	HadoopModel hadoopModel = procModel.getDestInfo();
  	String hiveDbName = hadoopModel.getHiveDbName();
  	String hiveTableName = hadoopModel.getHiveTableName();
  	Map<Integer, FieldModel> fieldsInfo = hadoopModel.getDestFieldInfo();
  	String hivePartition = hadoopModel.getHivePartition();
  	String processDesc = procModel.getNormalizedProcDesc();
  	String formatId = hadoopModel.getHadoopFormatCd();
    String compressId = hadoopModel.getHadoopCompressCd();
    
    String sql = createHiveTable(hiveDbName, hiveTableName, fieldsInfo, hivePartition, formatId, compressId, processDesc) + ";";
    logger.debug("Create Hive table SQL for process ID " + procModel.getProcId() + " is " + sql);
    return sql;
  }

	public String createDumpHiveTable(ProcessModel procModel) {
		HadoopModel hadoopModel = procModel.getDestInfo();
  	String hiveDbName = hadoopModel.getHiveDbName();
  	String hiveDumpTableName = StringUtils.trimToNull(hadoopModel.getHiveDumpTableName());
  	
  	FieldModel dumpField = new FieldModel();
  	dumpField.setRecordType(RecordType.FIELD_INFO);
  	dumpField.setFieldNum(1);
  	dumpField.setFieldName(dumpFieldName);
  	dumpField.setDataType(UobConstants.SRC_ALPHANUMERIC);
  	dumpField.setFieldDesc("Field to store records dumped from history file");
  	
  	Map<Integer, FieldModel> fieldsInfo = new HashMap<Integer, FieldModel>();
  	fieldsInfo.put(1, dumpField);
  	String hivePartition = hadoopModel.getHivePartition();
  	String processDesc = procModel.getNormalizedProcDesc();
  	String formatId = hadoopModel.getHadoopFormatCd();
    String compressId = hadoopModel.getHadoopCompressCd();
    
    String sql = createHiveTable(hiveDbName, hiveDumpTableName, fieldsInfo, hivePartition, formatId, compressId, processDesc);
    logger.debug("Create Hive dump table SQL for process ID " + procModel.getProcId() + " is " + sql);
    return sql;
	}
	
	private boolean checkForDifferentSourceFileFormat(String fileLayoutCD) {
		return (UobConstants.XLS_FILE_WITH_HEADER.equals(fileLayoutCD) 
				|| UobConstants.XLS_FILE_WITHOUT_HEADER.equals(fileLayoutCD) 
				|| UobConstants.XLSX_FILE_WITH_HEADER.equals(fileLayoutCD) 
				|| UobConstants.XLSX_FILE_WITHOUT_HEADER.equals(fileLayoutCD));
	}
}
