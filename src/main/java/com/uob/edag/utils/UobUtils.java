package com.uob.edag.utils;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.processor.BaseProcessor;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file contains the common utilities used in this project.
 * 
 */

public class UobUtils {
	
	private static Logger staticLogger = Logger.getLogger(UobUtils.class);
	private static final boolean PRINT_JAVA_CLASSPATH;
	
	static {
		boolean print = false;
		try {
			print = parseBoolean(PropertyLoader.getProperty(UobUtils.class.getName() + ".printJavaClasshpath"));
		} catch (EDAGValidationException e) {
			staticLogger.debug("Cannot get boolean value from property " + UobUtils.class.getName() + ".printJavaClasspath, defaulting to false");
		} finally {
			PRINT_JAVA_CLASSPATH = print;
		}
	}
	
	public static final String[] HIVE_KEYWORDS = {"ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION", 
      "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CAST", "CHAR", 
      "COLUMN", "CONF", "CREATE", "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", 
      "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DECIMAL", "DELETE", "DESCRIBE", 
      "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS", "EXTENDED", "EXTERNAL",
      "FALSE", "FETCH", "FLOAT", "FOLLOWING", "FOR", "FROM", "FULL", "FUNCTION", "GRANT", 
      "GROUP", "GROUPING", "HAVING", "IF", "IMPORT", "IN", "INNER", "INSERT", "INT", "INTERSECT", 
      "INTERVAL", "INTO", "IS", "JOIN", "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", 
      "MAP", "MORE", "NONE", "NOT", "NULL", "OF", "ON", "OR", "ORDER", "OUT", "OUTER", "OVER", 
      "PARTIALSCAN", "PARTITION", "PERCENT", "PRECEDING", "PRESERVE", "PROCEDURE", "RANGE", 
      "READS", "REDUCE", "REGEXP", "REVOKE", "RIGHT", "RLIKE", "ROLLUP", "ROW", "ROWS",
      "SELECT", "SET", "SMALLINT", "TABLE", "TABLESAMPLE", "THEN", "TIMESTAMP", "TO", 
      "TRANSFORM",  "TRIGGER", "TRUE", "TRUNCATE", "UNBOUNDED", "UNION", "UNIQUEJOIN", "UPDATE", 
      "USER", "USING", "VALUES", "VARCHAR", "WHEN", "WHERE", "WINDOW", "WITH"};
	
  /**
   * This method is used to a return a boolean value from a given string. It returns true if the
   *     value is Y or true or 1, It returns false if the value is N or false or 0
   * @param input The input string to be converted to boolean
   * @return the Boolean value
   * @throws Exception when there is an error converting to boolean
   */
  public static boolean parseBoolean(String input) throws EDAGValidationException {
    if (UobConstants.Y.equalsIgnoreCase(input) || 
    		UobConstants.TRUE.equalsIgnoreCase(input) ||
        UobConstants.ONE.equalsIgnoreCase(input)) {
      return true;
    } else if (UobConstants.N.equalsIgnoreCase(input) || 
    		       UobConstants.FALSE.equalsIgnoreCase(input) ||
               UobConstants.ZERO.equalsIgnoreCase(input) || 
               StringUtils.isBlank(input)) {
      return false;
    } else {
      throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, input, "Not a boolean value");
    }
  }

  /**
   * This method is used to return Y or N from a given boolean input value.
   * @param input The input boolean value
   * @return Y or N
   * @throws Exception when there is an error converting the boolean to text
   */
  public static String toBooleanChar(boolean input) {
  	return input ? UobConstants.Y : UobConstants.N;  
  }
  
  public static void logJavaProperties() {
  	logJavaProperties(null);
  }
  
  public static void logJavaProperties(Priority priority) {
  	Priority p = priority == null ? Level.INFO : priority;
  	staticLogger.log(p, "Java runtime name: " + SystemUtils.JAVA_RUNTIME_NAME);
    staticLogger.log(p, "Java runtime version: " + SystemUtils.JAVA_RUNTIME_VERSION);
    
    p = priority == null ? Level.DEBUG : priority;
    staticLogger.log(p, "Java specification name: " + SystemUtils.JAVA_SPECIFICATION_NAME);
    staticLogger.log(p, "Java specification vendor: " + SystemUtils.JAVA_SPECIFICATION_VENDOR);
    staticLogger.log(p, "Java specification version: " + SystemUtils.JAVA_SPECIFICATION_VERSION);
    staticLogger.log(p, "Java vendor: " + SystemUtils.JAVA_VENDOR);
    staticLogger.log(p, "Java version: " + SystemUtils.JAVA_VERSION);
    staticLogger.log(p, "Java VM info: " + SystemUtils.JAVA_VM_INFO);
    
    p = priority == null ? Level.INFO : priority;
    staticLogger.log(p, "Java VM name: " + SystemUtils.JAVA_VM_NAME);
    
    p = priority == null ? Level.DEBUG : priority;
    staticLogger.log(p, "Java VM specification name: " + SystemUtils.JAVA_VM_SPECIFICATION_NAME);
    staticLogger.log(p, "Java VM specification vendor: " + SystemUtils.JAVA_VM_SPECIFICATION_VENDOR);
    staticLogger.log(p, "Java VM specification version: " + SystemUtils.JAVA_VM_SPECIFICATION_VERSION);
    staticLogger.log(p, "Java VM vendor: " + SystemUtils.JAVA_VM_VENDOR);
    
    p = priority == null ? Level.INFO : priority;
    staticLogger.log(p, "Java VM version: " + SystemUtils.JAVA_VM_VERSION);
    
    p = priority == null ? Level.DEBUG : priority;
    if (PRINT_JAVA_CLASSPATH) {
    	staticLogger.log(p, "Java class path: " + SystemUtils.JAVA_CLASS_PATH);
    }
    staticLogger.log(p, "Java library path: " + SystemUtils.JAVA_LIBRARY_PATH);
  }
  
  public static void logPackageProperties() {
  	logPackageProperties(null);
  }
  
  public static void logPackageProperties(Priority priority) {
  	Package pkg = BaseProcessor.class.getPackage();
  	
  	Priority p = priority == null ? Level.DEBUG : priority;
  	staticLogger.log(p, "Package name: " + pkg.getName());
  	
  	p = priority == null ? Level.INFO : priority;
  	staticLogger.log(p, "Package implementation title: " + pkg.getImplementationTitle());
  	staticLogger.log(p, "Package implementation version: " + pkg.getImplementationVersion());
  	
  	p = priority == null ? Level.DEBUG : priority;
  	staticLogger.log(p, "Package implementation vendor: " + pkg.getImplementationVendor());
    staticLogger.log(p, "Package specification title: " + pkg.getSpecificationTitle());
    staticLogger.log(p, "Package specification version: " + pkg.getSpecificationVersion());
    staticLogger.log(p, "Package specification vendor: " + pkg.getSpecificationVendor());
  }

  /**
   * This method is used to check if a given input value is a Hive Reserved keyword.
   * @param keyword The input string value
   * @return true if the input is a keyword, false if its not a keyword
   */
  public static boolean checkHiveReservedKeyword(String keyword) {
    return Arrays.binarySearch(HIVE_KEYWORDS, keyword.toUpperCase()) >= 0;
  }
  
  public static String ltrim(String str) {
  	int i = 0;
    while (i < str.length() && Character.isWhitespace(str.charAt(i))) {
        i++;
    }
    return str.substring(i);
  }
  
  public static String quoteValue(Object value) {
  	return value == null ? "null" : "'" + value.toString() + "'";
  }
  
  public static boolean bypassImpalaForCustomSerDe(ProcessModel procModel) {
	  FileModel fileModel = procModel.getSrcInfo();
	  String fileLayoutId = StringUtils.trimToEmpty(fileModel.getSourceFileLayoutCd());
	  if(UobConstants.CSV.equalsIgnoreCase(fileLayoutId))
		  return true; 
	  return false;
  }
}
