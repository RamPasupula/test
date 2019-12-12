package com.uob.edag.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;

/**
 * Created by cs186076 on 4/8/17.
 */

import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.security.EncryptionUtil;
import com.uob.edag.utils.POIUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;
import com.uob.edag.utils.VelocityUtils;

public class PIISpecProcessor {
	
	  public static final String VIEW_TEMPLATE_PROPERTY = PIISpecProcessor.class.getName() + ".VIEW_TEMPLATE"; 
	  public static final String MAX_TS_VIEW_TEMPLATE_PROPERTY = PIISpecProcessor.class.getName() + ".MAX_TS_VIEW_TEMPLATE";
	  public static final String HISTORIC_VIEW_TEMPLATE_PROPERTY = PIISpecProcessor.class.getName() + ".HISTORIC_VIEW_TEMPLATE";
	
	  public enum SpecColumn {
	  	BATCH(1), SYSTEM(2), HIVE_TABLE_NAME(3), FILE_NAME(4), FIELD_NAME(5), PII_APPLICABILITY(6);
	  	
	  	private int insertParamIndex;
	  	
	  	SpecColumn(int insertParamIndex) {
	  		this.insertParamIndex = insertParamIndex;
	  	}
	  	
	  	public int getInsertParamIndex() {
	  		return insertParamIndex;
	  	}
	  };
	  
	  public class Table implements Comparable<Table> {
	  	private String name;
	  	private String loadType;
	  	
	  	private Table(String name, String loadType) {
	  		this.name = StringUtils.trimToEmpty(name);
	  		this.loadType = StringUtils.trimToEmpty(loadType);
	  	}
	  	
	  	public String getName() {
	  		return name;
	  	}
	  	
	  	public String getLoadType() {
	  		return loadType;
	  	}
	  	
	  	public boolean equals(Object obj) {
	  		boolean result = obj instanceof Table;
	  		
	  		if (result) {
	  			Table o  = (Table) obj;
	  			result = o.name.equals(this.name);
	  		}
	  		
	  		return result;
	  	}
	  	
	  	public int hashCode() {
	  		return this.name.hashCode();
	  	}

			@Override
			public int compareTo(Table o) {
				return o == null ? 1 : this.name.compareTo(o.name);
			}
	  }
	  
	  public class Field {
	  	private String name;
	  	private String description;
	  	
	  	private Field(String name) {
	  		this.name = StringUtils.trimToEmpty(name);
	  	}
	  	
	  	public boolean isNull() {
	  		return name.contains("<NULL>");
	  	}
	  	
	  	public String getNormalizedName() {
	  		return com.uob.edag.utils.StringUtils.normalizeForHive(name.replace("<NULL>", "")).toLowerCase();
	  	}
	  	
	  	public String getNormalizedDescription() {
	  		return com.uob.edag.utils.StringUtils.normalizeForHive(description, true);
	  	}
	  }

    protected Logger logger = Logger.getLogger(getClass());
    private static Connection connection = null;

    private static String TRUNCATE_SENSITIVE = "TRUNCATE TABLE EDAG_FIELD_SENSITIVE_DETAIL";
    //private static String TRUNCATE_DISPLAYDENY = "TRUNCATE TABLE EDAG_FIELD_DISPLAYDENY_DETAIL";
    private static String INSERT_SENSITIVE_TEMPLATE = "INSERT INTO EDAG_FIELD_SENSITIVE_DETAIL ( batch_num,src_sys_nm,hive_tbl_nm,file_nm,fld_nm  ) VALUES (?,?,?,?,?)";
    //private static String INSERT_DISPLAYDENY_TEMPLATE = "INSERT INTO EDAG_FIELD_DISPLAYDENY_DETAIL ( batch_num,src_sys_nm,hive_tbl_nm,file_nm,fld_nm   ) VALUES (?,?,?,?,?)";
    private static String
        SELECT_FIELD_TEMPLATE = "with k1 as (select  distinct elp.tgt_tbl_nm as tbl_nm,efid.fld_nm as fld_nm,efid.fld_num as fld_num from edag_process_master epm inner join edag_file_detail efd inner join edag_field_detail efid on efid.file_id = efd.file_id on efd.proc_id = epm.proc_id inner join edag_load_process elp on elp.proc_id = epm.proc_id inner join edag_proc_ctry_dtl epcd on epcd.proc_id = epm.proc_id ) select distinct efsd.hive_tbl_nm, efsd.fld_nm from edag_field_sensitive_detail efsd where not exists( select 1 from k1 where k1.tbl_nm = efsd.HIVE_TBL_NM and k1.fld_nm = efsd.fld_nm )";
    private static final String REGISTER_OPTION = "register";
    private static final String NS_VIEWS_OPTION = "ns_views";
    private static final String S_VIEWS_OPTION = "s_views";
    private static final String GENERIC_NS_VIEWS_OPTION = "generic_ns_views";
    private static final String GENERIC_S_VIEWS_OPTION = "generic_s_views";
    private static final String REPL_NS_VIEWS_OPTION = "nonsensitive_views";
    private static final String REPL_S_VIEWS_OPTION = "sensitive_views";
    private static String option_selected = "";
    private static String option_displayed = "";
    private static String option_replaced = "";
    static int piicounter_oo = 0;
    static String fieldsOutFilePath = "";

    public PIISpecProcessor(String driver, String conn, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        connection = DriverManager.getConnection(conn, username, password);
    }
    
    protected void finalize() throws Throwable {
    	connection.close();
    }

    public static void main(String[] args) throws Exception {
        DateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
        String execDate = formatter.format(System.currentTimeMillis());
        String procId = args[0];
        String logFileName = "EDA_DDS_DL_" + procId + "_" + execDate + ".log";
        System.setProperty("logFileName", logFileName);
        UobUtils.logJavaProperties();
        UobUtils.logPackageProperties();
        String driverName = PropertyLoader.getProperty("JDBC.Driver");
        String driverConn = PropertyLoader.getProperty("JDBC.ConnectionURL");
        String userName = PropertyLoader.getProperty("JDBC.Username");
        String password = new EncryptionUtil().decrypt(PropertyLoader.getProperty("JDBC.Password"));
        String outfilePath = PropertyLoader.getProperty("SQL_FILE_LOCATION");
        String outfilename = PropertyLoader.getProperty("SQL_FILE_DDS_NAME");
        String sqlfilepath = "";
        String srcsyscd = "all";
        fieldsOutFilePath = PropertyLoader.getProperty("SQL_FILE_DDS_NAME");

        if (args[0].equals(REGISTER_OPTION)) {
        	  boolean truncateInsert = false;
        	  String filePath = null;
        	  Map<SpecColumn, Integer> columnMap = null;
        	  
        	  try {
		            filePath = args[1];
		            if (StringUtils.trimToNull(args[2]) == null) {
		            	throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "delete all flag", "valid delete all flag values are 'true' and'false'");
		            }
		            truncateInsert = UobUtils.parseBoolean(args[2]);
		            String numbersStr = PropertyLoader.getProperty(PIISpecProcessor.class.getName() + ".FieldIndexes");
		            if (args.length > 3) {
		            	numbersStr = args[3];
		            }
		            
		            columnMap = getColumnIndexMap(numbersStr);
        	  } catch (Exception e) {
        	  	  System.out.println("Registration fails because of the following error: " + e.getMessage());
          	  	System.out.println("Registration requires the following parameters:");
          	  	System.out.println("1. Sensitive field specification Excel file to be processed");
          	  	System.out.println("2. Delete all from sensitive field table (true / false)");
          	  	System.out.println("3. PII spec field indexes (comma-separated values, optional)");
          	  	System.exit(-1);
        	  }
        	  
        	  new PIISpecProcessor(driverName, driverConn, userName, password).register(truncateInsert, filePath, columnMap);
        } else if (args[0].equals(NS_VIEWS_OPTION) || args[0].equals(S_VIEWS_OPTION)) {
	        	if (args.length < 3) {
	          	System.out.println("ns_views / s_views operations require 2 parameters: "); 
	          	System.out.println("1. SQL file to generate views from metadata ");
	          	System.out.println("2. Source system whose views will be generated (use 'all' for all source systems) ");
	          	System.exit(-1);
	          }
        	
            //<source_system>_<option>
            if (args[1] != null) {
                sqlfilepath = args[1];
            } else {
                throw new EDAGException("Please provide sql file to generate views from metadata");
            }
            
            if (args[0].equals(NS_VIEWS_OPTION)) {
                if (!StringUtils.equals(args[2].toLowerCase(), "all")) {
                    srcsyscd = args[2];
                    option_selected = GENERIC_NS_VIEWS_OPTION;
                    option_displayed = NS_VIEWS_OPTION;
                    option_replaced = REPL_NS_VIEWS_OPTION;
                } else {
                    System.err.println("Since no source system is specified, reading all source systems.");
                    option_selected = NS_VIEWS_OPTION;
                    option_displayed = NS_VIEWS_OPTION;
                    option_replaced = REPL_NS_VIEWS_OPTION;
                }
            } else if (args[0].equals(S_VIEWS_OPTION)) {
                if (!StringUtils.equals(args[2].toLowerCase(), "all")) {
                    srcsyscd = args[2];
                    option_selected = GENERIC_S_VIEWS_OPTION;
                    option_displayed = S_VIEWS_OPTION;
                    option_replaced = REPL_S_VIEWS_OPTION;
                } else {
                    System.err.println("Since no source system is specified, reading all source systems.");
                    option_selected = S_VIEWS_OPTION;
                    option_displayed = S_VIEWS_OPTION;
                    option_replaced = REPL_S_VIEWS_OPTION;
                }
            } else {
                throw new EDAGException("Please provide valid command : valid commands are ns_views/s_views");
            }
            outfilename = outfilename.replaceAll("<source_system>", srcsyscd);
            outfilename = outfilename.replaceAll("<option>", option_replaced);
            outfilePath = outfilePath + "/" + outfilename;
            fieldsOutFilePath = outfilePath + "_fields";
            new PIISpecProcessor(driverName, driverConn, userName, password).createSQLFile(outfilePath, sqlfilepath, srcsyscd);

        } else {
            throw new EDAGException("Please provide valid option as first parameter to generate SQL ,options are register, ns_views , s_views");
        }
    }

    private static Map<SpecColumn, Integer> getColumnIndexMap(String value) throws EDAGValidationException {
    	  Map<SpecColumn, Integer> result = new HashMap<SpecColumn, Integer>();
    	  
    	  try {
	    	  String[] strs = value.split(",", -1);
	    	  int i = 0;
	    	  for (SpecColumn specColumn : SpecColumn.values()) {
	    	  	result.put(specColumn, Integer.parseInt(strs[i]));
	    	  	i++;
	    	  }
    	  } catch (IndexOutOfBoundsException e) {
    	  	throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, value, "PII spec field indexes for " + ArrayUtils.toString(SpecColumn.values()) + " must be supplied as comma-separated values");
    	  }
    	  
        return result;
    }

    /**
     * args0 : FilePath for the EdagSensitiveFields List.
     */

    public void register(boolean truncateInsert, String filePath, Map<SpecColumn, Integer> columnMap) throws EDAGException {
    	logger.info("Going to parse Sensitive Field Specifications for file: " + filePath);
    	try (OPCPackage pkg = OPCPackage.open(filePath)) {
        Workbook wb = WorkbookFactory.create(pkg);
        
        try (PreparedStatement ps = connection.prepareStatement(INSERT_SENSITIVE_TEMPLATE)) {
        	  logger.debug(INSERT_SENSITIVE_TEMPLATE + " prepared");
        	  
            // Parse the Process Downstream Application Sheet
            Sheet sheet = wb.getSheetAt(0);
            if (sheet == null) {
                throw new EDAGException("There is no Process Downstream Application Sheet in the Process Specification");
            } else {
            	logger.info("Processing worksheet " + sheet.getSheetName());
            }
            
            if (truncateInsert) {
            	try (PreparedStatement truncatePs = connection.prepareStatement(TRUNCATE_SENSITIVE)) {
            		truncatePs.execute();
            		logger.debug(TRUNCATE_SENSITIVE + " executed");
            	}
            }

            int piicounter = 0;
            int piiApplicabilityIndex = columnMap.get(SpecColumn.PII_APPLICABILITY);
            int batchIndex = columnMap.get(SpecColumn.BATCH);
            int sourceSystemIndex = columnMap.get(SpecColumn.SYSTEM);
            int hiveTableIndex = columnMap.get(SpecColumn.HIVE_TABLE_NAME);
            int fileNameIndex = columnMap.get(SpecColumn.FILE_NAME);
            int fieldNameIndex = columnMap.get(SpecColumn.FIELD_NAME);
            ConcurrentHashMap<String, Set<String>> cmapset = new ConcurrentHashMap<String, Set<String>>();
            Iterator<Row> rowIterator = sheet.rowIterator();
            boolean headerRow = true;
            while (rowIterator.hasNext()) {
            	Row xssfrow = rowIterator.next(); 
            	
            	if (!isRowEmpty(xssfrow)) {
	            	if (!headerRow) { 
	                  boolean PIIavailable = "Y".equalsIgnoreCase(POIUtils.getCellContent(xssfrow, piiApplicabilityIndex, String.class));
	                  if (PIIavailable) {
	                  	  int rowNum = xssfrow.getRowNum();
	                  	  int batchNo = POIUtils.getCellContent(xssfrow, batchIndex, Integer.class);
	                  	  if (batchNo <= 0) {
	                  	  	throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, batchNo, "Invalid batch number on row " + rowNum);
	                  	  }
	                  	  
	                  	  String sourceSystem = POIUtils.getCellContent(xssfrow, sourceSystemIndex, String.class);
	                  	  checkBlankValue(sourceSystem, "source system code", rowNum);
	                  	  
	                  	  String hiveTableName = POIUtils.getCellContent(xssfrow, hiveTableIndex, String.class);
	                  	  checkBlankValue(hiveTableName, "Hive table name", rowNum);
	                  	  
	                  	  String fileName = POIUtils.getCellContent(xssfrow, fileNameIndex, String.class);
	                  	  checkBlankValue(fileName, "file name", rowNum);
	                  	  
	                  	  String fieldName = POIUtils.getCellContent(xssfrow, fieldNameIndex, String.class);
	                  	  checkBlankValue(fieldName, "field name", rowNum);
	                  	  
	                  	  ps.setInt(SpecColumn.BATCH.getInsertParamIndex(), batchNo);
	                  	  ps.setString(SpecColumn.SYSTEM.getInsertParamIndex(), sourceSystem);
	                  	  ps.setString(SpecColumn.HIVE_TABLE_NAME.getInsertParamIndex(), hiveTableName);
	                  	  ps.setString(SpecColumn.FILE_NAME.getInsertParamIndex(), fileName);
	                  	  ps.setString(SpecColumn.FIELD_NAME.getInsertParamIndex(), fieldName);
	                  	  
	                  	  Set<String> fields = cmapset.get(hiveTableName);
	                  	  if (fields == null) {
	                  	  	fields = new TreeSet<String>();
	                  	  	cmapset.put(hiveTableName, fields);
	                  	  }
	                  	  fields.add(fieldName);
                        
	                      ps.addBatch();
	                      if (++piicounter % 1000 == 0) {
	                          ps.executeBatch();
	                          connection.commit();
	                          logger.info(piicounter + " rows executed");
	                      }
	                  }
	            	} else {
	            		Iterator<Cell> cellIterator = xssfrow.cellIterator();
	            		while (cellIterator.hasNext()) {
	            			Cell cell = cellIterator.next();
	            			logger.debug("Header " + cell.getColumnIndex() + ": " + cell.toString());
	            		}
	            		headerRow = false;
	            	}
            	}
            }
            
            ps.executeBatch();
            String msg = "Registered successfully " + piicounter + " PII fields";
            logger.info(msg);
            System.out.println(msg);
            connection.commit();
            validateSensitiveField(cmapset);
        } 
    	} catch (Exception e) {
        throw new EDAGException(e.getMessage());
      }  
    }

    private void checkBlankValue(String value, String fieldName, int rowNumber) throws EDAGValidationException {
			if (StringUtils.isBlank(value)) {
				throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, fieldName, "Row number is " + rowNumber);
			}
		}

		private boolean isRowEmpty(Row row) {
			if (row == null || row.getLastCellNum() <= 0) {
				return true;
			}
			
			boolean empty = true;
			Iterator<Cell> cellIterator = row.cellIterator();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				if (cell != null && cell.getCellTypeEnum() != CellType.BLANK && StringUtils.isNotBlank(cell.toString())) {
					empty = false;
					break;
				}
			}
			
			return empty;
		}

		private boolean validateSensitiveField(ConcurrentHashMap<String, Set<String>> cmapset) throws EDAGException {
        Enumeration<String> keys = cmapset.keys();
        final ExecutorService executor = Executors.newCachedThreadPool();
        ArrayList<Future<Boolean>> alFutures = new ArrayList<Future<Boolean>>();
        StringBuilder sb = new StringBuilder();
        while (keys.hasMoreElements()) {
            String tableLeft = keys.nextElement();
            sb.append("'").append(tableLeft).append("'");
        }
        String tableset = sb.toString();
        logger.debug("Table set: " + tableset);
        alFutures.add(executor.submit(new SqlTask(SELECT_FIELD_TEMPLATE)));

        try {
	        for (Future<Boolean> fb : alFutures) {
	            if (!fb.get()) {
	                return false;
	            } else {
	                logger.debug("Processed ");
	            }
	        }
        } catch (InterruptedException | ExecutionException e) {
        	throw new EDAGException(e.getMessage());
        }
        
        return true;
    }

    public void createSQLFile(String outfilepath, String sqlfilepath, String srcSysCode) throws EDAGException {
    	try {
        FileInputStream fis = new FileInputStream(sqlfilepath);
        logger.debug("Input stream opened from " + sqlfilepath);
        String fileContent = getFileContent(fis, Charset.defaultCharset().toString());
        String st = fileContent.replaceAll("\n", " ");
        PreparedStatement ps5 = connection.prepareStatement(st);
        logger.debug(st + " prepared");
        if (!StringUtils.equals(srcSysCode.toLowerCase(), "all")) {
            ps5.setString(1, srcSysCode);
            if (option_selected.equals(GENERIC_NS_VIEWS_OPTION)) {
                ps5.setString(2, srcSysCode);
            }
        }
        ResultSet rs = ps5.executeQuery();
        ConcurrentHashMap<String, SortedSet<Table>> ctryTableMap = new ConcurrentHashMap<String, SortedSet<Table>>();
        ConcurrentHashMap<Table, SortedMap<Integer, Field>> tableFieldMap = new ConcurrentHashMap<>();
        try {
            while (rs.next()) {
                SortedMap<Integer, Field> ll = null;
                String tableName = null;
                String ctrycode = null;
                ctrycode = rs.getString("ctry_cd");
                tableName = rs.getString("tbl_nm");
                Field field = new Field(rs.getString("fld_nm"));
                try {
                  field.description = rs.getString("fld_desc");
                } catch (SQLException e) {
                	// means fld_desc column is not specified in SQL statement.
                }
                int colOrder = rs.getInt("fld_num");
                String loadType = rs.getString("tgt_aply_type_cd");
                Table table = new Table(tableName, loadType);
                ll = tableFieldMap.getOrDefault(table, new TreeMap<Integer, Field>());
                ll.put(colOrder, field);
                tableFieldMap.put(table, ll);

				//!---- Reverted PII to Phase 1, sm186140 --->//

				if (!ctrycode.equals("ID") && !ctrycode.equals("MY") && !ctrycode.equals("TH") && !ctrycode.equals("CN")) {
					SortedSet<Table> tblList1 = ctryTableMap.getOrDefault("so", new TreeSet<Table>());
					tblList1.add(table);
					ctryTableMap.put("so", tblList1);
				} else if (ctrycode.equals("MY")) {
					SortedSet<Table> tblList2 = ctryTableMap.getOrDefault("my", new TreeSet<Table>());
					tblList2.add(table);
					ctryTableMap.put("my", tblList2);
				} else if (ctrycode.equals("TH")) {
					SortedSet<Table> tblList3 = ctryTableMap.getOrDefault("th", new TreeSet<Table>());
					tblList3.add(table);
					ctryTableMap.put("th", tblList3);
				} else if (ctrycode.equals("CN")) {
					SortedSet<Table> tblList4 = ctryTableMap.getOrDefault("cn", new TreeSet<Table>());
					tblList4.add(table);
					ctryTableMap.put("cn", tblList4);
				} else if (ctrycode.equals("ID")) {
					SortedSet<Table> tblList5 = ctryTableMap.getOrDefault("id", new TreeSet<Table>());
					tblList5.add(table);
					ctryTableMap.put("id", tblList5);
				}
				if (!ctrycode.equals("ID")) {
					SortedSet<Table> tblList6 = ctryTableMap.getOrDefault("gd", new TreeSet<Table>());
					tblList6.add(table);
					ctryTableMap.put("gd", tblList6);
				}
				// ---- End of PII Revert --->//

			}
            
            generateSQL(ctryTableMap, tableFieldMap, outfilepath, srcSysCode);
        } finally {
        	try {
            rs.close();
            ps5.close();
        	} catch (SQLException e) {
        		logger.warn("Unable to close result set / statement: " + e.getMessage());
        	}
        }
    	} catch (Exception e) {
    		throw new EDAGException(e.getMessage());
    	}
    }

    private void generateSQL(ConcurrentHashMap<String, SortedSet<Table>> mapsList, ConcurrentHashMap<Table, SortedMap<Integer, Field>> tableFieldMap, String outfilePath, String srcSysCode) throws EDAGIOException {
    	  logger.debug("Starting SQL generation");
    	  String template = VelocityUtils.getTemplateFromResource(PropertyLoader.getProperty(VIEW_TEMPLATE_PROPERTY), true);
    	  String historicTemplate = VelocityUtils.getTemplateFromResource(PropertyLoader.getProperty(HISTORIC_VIEW_TEMPLATE_PROPERTY), true);
    	
        Enumeration<String> keysMap = mapsList.keys(); // map of country list
        
        List<String> cmapfieldsList = new ArrayList<String>();
        boolean closingWriter = false;
        try (Writer writer = new FileWriter(outfilePath)) {
        	while (keysMap.hasMoreElements()) {
        		int piiFieldCounter = 0;
        		String key = keysMap.nextElement();
        		SortedSet<Table> tables = mapsList.get(key);
        		logger.debug("Group Name : " + key + " Views Count : " + tables.size());
        		
        		for (Table tbl : tables) {
        			int counter = 0;
        			Context ctx = new VelocityContext();
        			String viewPrefix = "[ENV]_" + key + "_" + option_displayed.split("_")[0] + "_v.";
        			boolean historyLoad = "HST".equalsIgnoreCase(tbl.loadType);
        			Collection<Field> fields = tableFieldMap.get(tbl).values();
        			if ("all".equalsIgnoreCase(srcSysCode) || StringUtils.containsIgnoreCase(tbl.name, srcSysCode)) {
        				for (Field field : fields) {
        					cmapfieldsList.add(key + "," + tbl.name + "," + field.getNormalizedName());
        					counter++;
        				}
        			}
        			
        			piiFieldCounter = piiFieldCounter + counter;
        			ctx.put("fields", fields);
        			ctx.put("tableName", "[ENV]_" + tbl.name.substring(4, 7) + "." + tbl.name);
        			ctx.put("siteIdConditionKey", key);
        			
        			StringWriter strWriter;
        			
        			if (historyLoad) {
        				ctx.put("viewName", viewPrefix + tbl.name + "_h");
        				strWriter = new StringWriter();
        				VelocityUtils.evaluate(ctx, strWriter, ctx.get("viewName").toString(), template);
        				writer.write(strWriter.toString() + System.lineSeparator());
        			}
        			
        			ctx.put("viewName", viewPrefix + tbl.name);
        		  	strWriter = new StringWriter();
        			VelocityUtils.evaluate(ctx, strWriter, ctx.get("viewName").toString(), historyLoad ? historicTemplate : template);
        			writer.write(strWriter.toString() + System.lineSeparator()); 
        		}
        		
        		logger.debug("PII Fields Counter for Group : " + key + " with counter " + piiFieldCounter);
        	}
        	
        	closingWriter = true;
        } catch (IOException e) {
					if (closingWriter) {
						logger.warn("Unable to close writer to " + outfilePath + ": " + e.getMessage());
					} else {
						throw new EDAGIOException(EDAGIOException.CANNOT_WRITE_FILE, outfilePath, e.getMessage());
					}
				}
        
        writeToFile(cmapfieldsList, fieldsOutFilePath);
        String msg = "SQL File Generated Successfully and written at " + outfilePath +
                     "and fields written at " + fieldsOutFilePath;
        logger.info(msg);
        System.out.println(msg);
    }

		public void writeToFile(List<String> as, String filename) {
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(filename);
            bw = new BufferedWriter(fw);
            for (String s : as) {
                bw.write(s + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        
        logger.debug(as.size() + " lines written into " + filename);
    }

    public void writeSetToFile(TreeSet<String> as, String filename) {
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(filename);
            bw = new BufferedWriter(fw);
            for (String s : as) {
                bw.write(s + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        
        logger.debug(as.size() + " lines written into " + filename);
    }

    private static String getFileContent(
        FileInputStream fis,
        String encoding) throws IOException {
        try (BufferedReader br =
                 new BufferedReader(new InputStreamReader(fis, encoding))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append('\n');
            }
            return sb.toString();
        }
    }

    public class SqlTask implements Callable<Boolean> {

        private String sql;


        public SqlTask(final String sql) {
            this.sql = sql;
        }

        @Override
        public Boolean call() throws Exception {
            String driverName = PropertyLoader.getProperty("JDBC.Driver");
            String driverConn = PropertyLoader.getProperty("JDBC.ConnectionURL");
            String userName = PropertyLoader.getProperty("JDBC.Username");
            String password = new EncryptionUtil().decrypt(PropertyLoader.getProperty("JDBC.Password"));
            Class.forName(driverName);
            Connection connection_1 = DriverManager.getConnection(driverConn, userName, password);
            PreparedStatement stmt = connection_1.prepareStatement(this.sql);
            ResultSet rsSelect = stmt.executeQuery();
            try {
                while (rsSelect.next()) {
                    String tableRight = rsSelect.getString(1);
                    String fieldsRight = rsSelect.getString(2);
                    System.out.println("Sensitive Fields " + fieldsRight + " for table : " + tableRight + " does not exist.Please correct them manually.");
                }
            } catch (Exception e) {
                return false;
            }finally{
                rsSelect.close();
                stmt.clearParameters();
                stmt.close();
                connection_1.close();
            }
            return true;
        }
    }
}
