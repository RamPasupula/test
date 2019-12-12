package com.uob.edag.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGPOIException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.FieldModel.RecordType;
import com.uob.edag.model.InterfaceSpec;
import com.uob.edag.model.LoadProcess;
import com.uob.edag.model.LobStampingInfo;
import com.uob.edag.model.ProcDownstreamAppl;
import com.uob.edag.model.ProcParam;
import com.uob.edag.model.ProcessMaster;
import com.uob.edag.model.SourceTableDetail;

import jline.internal.Log;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for parsing the Interface Spec File provided
 *              by UOB. The Bulk Registration Utility to register the Ingestion
 *              Files, will be based on this.
 * 
 */

public class UobInterfaceFileParser {
	
	public static final Set<String> VALID_CHARSETS = new HashSet<String>();
  public static final Set<String> VALID_ENCODINGS = new HashSet<String>();
  
  private final static Logger staticLogger = Logger.getLogger(UobInterfaceFileParser.class);
  
  static {
  	VALID_CHARSETS.add("default");
  	try {
  		for (String key : PropertyLoader.getAllPropertyKeys(null, UobConstants.CHARSET_SUFFIX)) {
  			VALID_CHARSETS.add(PropertyLoader.getProperty(key));
  		}
  	}	catch (EDAGIOException e) {
  		staticLogger.warn("Unable to get all valid charset properties: " + e.getMessage());
  	}
  	
  	VALID_ENCODINGS.add("default");
  	try {
  		for (String key : PropertyLoader.getAllPropertyKeys(null, UobConstants.ENCODING_SUFFIX)) {
  			VALID_ENCODINGS.add(PropertyLoader.getProperty(key));
  		}
  	} catch (EDAGIOException e) {
  		staticLogger.warn("Unable to get all valid encoding properties: " + e.getMessage());
  	}
  }
	
	private class CountryAttributes {
		String countryCode;
		boolean supported = false;
		String charset = "default";
		String encoding = "default";
		String referencedFileSourceFolder = null;
	}
	
	protected Logger logger = Logger.getLogger(getClass());

	/**
	 * This method is used to parse the Specification File Inputs.
	 * 
	 * @param filePath
	 *            The Path of the Interface Specification on the Server
	 * @param procSpecPath
	 *            The Path of the Process Specification file on the Server
	 * @return the Interface Specification object
	 * @throws Exception
	 *             when there is any error parsing the interface or process
	 *             specification
	 */
	public InterfaceSpec parseInterfaceSpecFile(String filePath, String procSpecPath) throws EDAGPOIException, EDAGValidationException {
		logger.debug("Parsing Interface Spec File: " + filePath);
		InterfaceSpec interSpec = null;
		interSpec = parseSourceFileSpecification(filePath);
		if (interSpec.getSrcFileSpec() == null || interSpec.getSrcFileSpec().size() == 0) {
			logger.debug("Interface spec file is empty");
			return null;
		}
		
		Map<String, Map<String, InterfaceSpecMap>> processSpec = parseProcessSpecification(procSpecPath, filePath);
		Map<String, Map<String, InterfaceSpecMap>> srcFieldSpec = parseSourceFieldSpecification(filePath, interSpec);

		File file = new File(filePath);
		String fileName = file.getName();
		interSpec.setInterfaceSpecName(fileName);
		String srcSystem = fileName.substring(4, 7);
		interSpec.setSrcSystem(srcSystem);
		interSpec.setProcessSpec(processSpec);
		interSpec.setSrcFieldSpec(srcFieldSpec);

		if (interSpec.getCtrlFileSpec() != null) {
			Map<String, InterfaceSpecMap> ctrlFieldSpec = parseControlFieldSpecification(filePath, interSpec);
			interSpec.setCtrlFieldSpec(ctrlFieldSpec);
		}
		
		Map<String, Map<String, String>> processParamSpec = parseProcessParamSpecification(procSpecPath);
		interSpec.setParamSpec(processParamSpec);
		Map<String, String> processDownstreamSpec = parseProcessDownstreamSpecification(procSpecPath);
		interSpec.setDownstreamSpec(processDownstreamSpec);
		logger.debug("Parsed Interface Spec is: " + interSpec);
		
		return interSpec;
	}

	/**
	 * This method is used to parse the Process Specification Params excel
	 * sheet.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @return the parsed values in a HashMap
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	private Map<String, Map<String, String>> parseProcessParamSpecification(String filePath) throws EDAGPOIException, EDAGValidationException {
		int row = 0;
		int col = 0;
		HashMap<String, Map<String, String>> procSpecInfo = null;
		XSSFWorkbook wb;
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Param Specifications Params for file: " + filePath);
		} catch (IOException e1) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e1.getMessage());
		}
		
		int lastProcessedRow = 0;
		int processedRowCount = 0;
		try {
			// Parse the Data Ingestion Params Sheet
			XSSFSheet sheet = wb.getSheet(UobConstants.DATA_INGEST_PARAM_SPEC);
			if (sheet == null) {
				throw new EDAGPOIException("There is no Data Ingestion Param Sheet in Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				if (row < 1) {
					continue; // Header Line
				} else {
					XSSFRow xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.debug("Skipping Empty Row: " + row);
						continue;
					}
				}
				
				if (procSpecInfo == null) {
					procSpecInfo = new HashMap<String, Map<String, String>>();
				}

				String procId = POIUtils.getCellContent(sheet, row, col++, String.class);

				if (StringUtils.isEmpty(procId)) {
					continue; // Skip the row
				}
				
				String parameterName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String parameterValue = POIUtils.getCellContent(sheet, row, col++, String.class);

				Map<String, String> rowData = procSpecInfo.get(procId);
				if (rowData == null) {
					rowData = new HashMap<String, String>();
					procSpecInfo.put(procId, rowData);
				}
						
				rowData.put(parameterName, parameterValue);

				lastProcessedRow = row + 1;
				processedRowCount++;
			}
			
			return procSpecInfo;
		} finally {
			logger.debug("Parsed " + processedRowCount + " from process param spec sheet, last processed row is row " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				Log.warn("Unable to close workbook: " + e.getMessage());
			}
		}
	}

	/**
	 * This method is used to parse the Process Downstream Mapping Specification
	 * excel sheet.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @return the parsed values in a HashMap
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	private Map<String, String> parseProcessDownstreamSpecification(String filePath) throws EDAGPOIException, EDAGValidationException {
		XSSFWorkbook wb = null;
		int row = 0;
		int col = 0;
		Map<String, String> procSpecInfo = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Downstream Specifications for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int lastProcessedRow = 0;
		int processedRowCount = 0;
		try {
			// Parse the Process Downstream Application Sheet
			XSSFSheet sheet = wb.getSheet(UobConstants.PROC_DOWNSTREAM_SPEC);
			if (sheet == null) {
				throw new EDAGPOIException("There is no Process Downstream Application Sheet in the Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.info("Number of Rows in Sheet: " + noOfRows);
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				if (row < 1) {
					continue; // Header Line
				} else {
					XSSFRow xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.info("Skipping Empty Row: " + row);
						continue;
					}
				}
				
				if (procSpecInfo == null) {
					procSpecInfo = new HashMap<String, String>();
				}

				String procId = POIUtils.getCellContent(sheet, row, col++, String.class);

				if (StringUtils.isEmpty(procId)) {
					continue; // Skip the row
				}

				String downstreamApplNm = POIUtils.getCellContent(sheet, row, col++, String.class);

				if (procSpecInfo.containsKey(procId)) {
					throw new EDAGPOIException("Duplicate Downstream Specifications for Process: " + procId);
				}

				procSpecInfo.put(procId, downstreamApplNm);

				lastProcessedRow = row + 1;
				processedRowCount++;
			}
			
			return procSpecInfo;
		} finally {
			logger.debug(processedRowCount + " processed successfully from Process Downstream Spec Sheet, last processed row number: " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
	}

	/**
	 * This method is used to parse the Process Specification excel file.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @param interfaceSpecPath
	 *            The File Path of the Interface Specification
	 * @return the parsed values in a HashMap
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	private Map<String, Map<String, InterfaceSpecMap>> parseProcessSpecification(String filePath, 
			                                                                         String interfaceSpecPath) throws EDAGPOIException, EDAGValidationException {

		XSSFWorkbook wb = null;
		int row = 0;
		int col = 0;
		Map<String, Map<String, InterfaceSpecMap>> procSpecInfo = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Specifications for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int lastProcessedRow = 0;
		int processedRowCount = 0;
		try {
			// Parse the Data Ingestion Sheet
			XSSFSheet sheet = wb.getSheet(UobConstants.DATA_INGESTION_SPEC);
			if (sheet == null) {
				throw new EDAGPOIException("There is no \"DataIngestion\" Sheet in the Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				if (row < 1) {
					continue; // Header Line
				} else {
					XSSFRow xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.debug("Skipping Empty Row: " + row);
						continue;
					}
				}
				
				if (procSpecInfo == null) {
					procSpecInfo = new HashMap<String, Map<String, InterfaceSpecMap>>();
				}

				String serNo = POIUtils.getCellContent(sheet, row, col++, String.class);

				if (StringUtils.isBlank(serNo)) {
					logger.debug("Skipping Empty Row: " + row);
					continue;
				}
				
				String srcSysName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String interfaceSpecName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String fileName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String procId = POIUtils.getCellContent(sheet, row, col++, String.class);
				String procName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String tableName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String procGroup = POIUtils.getCellContent(sheet, row, col++, String.class);
				String t1Partition = POIUtils.getCellContent(sheet, row, col++, String.class);
				String t11Partition = POIUtils.getCellContent(sheet, row, col++, String.class);
				double threshold = POIUtils.getCellContent(sheet, row, col++, Double.class);
				String criticality = POIUtils.getCellContent(sheet, row, col++, String.class);
				String deployEdgeNode = POIUtils.getCellContent(sheet, row, col++, String.class);
				String alertEmail = POIUtils.getCellContent(sheet, row, col++, String.class);

				InterfaceSpecMap rowData = new DefaultInterfaceSpecMap();
				rowData.put(UobConstants.SNO, serNo);
				rowData.put(UobConstants.SRC_SYS_NM, srcSysName);
				rowData.put(UobConstants.INTERFACE_SPEC_NM, interfaceSpecName);
				rowData.put(UobConstants.FILE_NM, fileName);
				rowData.put(UobConstants.PROCESS_ID, procId);
				rowData.put(UobConstants.PROCESS_NAME, procName);
				rowData.put(UobConstants.TABLE_NAME, tableName);
				rowData.put(UobConstants.PROCESS_GRP, procGroup);
				rowData.put(UobConstants.T1_PARTITION, t1Partition);
				rowData.put(UobConstants.T11_PARTITION, t11Partition);
				rowData.put(UobConstants.ERROR_THRESHOLD, threshold);
				rowData.put(UobConstants.PROCESS_CRITICALITY, criticality);
				rowData.put(UobConstants.DEPLOYMENT_NODE, deployEdgeNode);
				rowData.put(UobConstants.ALERT_EMAIL, alertEmail);

				if (procSpecInfo.containsKey(fileName)) {
					throw new EDAGPOIException("Duplicate Process Specifications for Source File: " + fileName);
				}

				if (procSpecInfo.containsKey(interfaceSpecName)) {
					Map<String, InterfaceSpecMap> fileProcData = procSpecInfo.get(interfaceSpecName);
					fileProcData.put(fileName, rowData);
					procSpecInfo.put(interfaceSpecName, fileProcData);
				} else {
					Map<String, InterfaceSpecMap> fileProcData = new HashMap<String, InterfaceSpecMap>();
					fileProcData.put(fileName, rowData);
					procSpecInfo.put(interfaceSpecName, fileProcData);
				}

				lastProcessedRow = row + 1;
				processedRowCount++;
			}
			
			return procSpecInfo;
		} finally {
			logger.debug(processedRowCount + " parsed successfully from Process Spec Sheet, last processed row number is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
	}
	
	/**
	 * This method is used to parse the Process Specification excel file.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @return the processMaster objects in an ArrayList
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	public Map<String, ProcessMaster> parseProcessSpecification(String filePath) throws EDAGPOIException {
		XSSFWorkbook wb = null;
		int rw = 0;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Specifications for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		Map<String, ProcessMaster> processMastersMap = new HashMap<String, ProcessMaster>();
		int lastProcessedRow = 0;
		int processedRowCount = 0;
		try {
			// Parse the Data Loading Sheet
			XSSFSheet sheet = wb.getSheet(UobConstants.DATA_LOADING);
			if (sheet == null) {
				throw new EDAGPOIException("There is no \"" + UobConstants.DATA_LOADING + "\" Sheet in the Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.info("Number of Rows in Sheet: " + noOfRows);
			
			ProcessMaster processMaster = null;
			LoadProcess loadProcess = null;
			SourceTableDetail sourceTableDetail = null;
			
			String textValue = null;
			int numericValue = 0;
			
			Cell cel = null;
			for (Row row: sheet) {
				rw = row.getRowNum();
				cel = row.getCell(0);
				
				// Traversing through empty or header rows.
				switch (cel.getCellTypeEnum()) {
				case STRING:
					textValue = cel.getRichStringCellValue().getString();
					if (textValue.trim().equalsIgnoreCase(UobConstants.SERIAL_NUMBER)) {
						continue; // header row
					}
					
					break;
				default:
					break;
				}
				
				processMaster = new ProcessMaster();
				loadProcess = new LoadProcess();
				sourceTableDetail = new SourceTableDetail();
				
				processMaster.setIsActFlg('Y');
				for (Cell c : row) {
					switch(c.getCellTypeEnum()) {
					case STRING:
						if (c.getColumnIndex() == 1) {
							textValue = c.getRichStringCellValue().getString();
							processMaster.setSrcSysCd(textValue);
						} else if (c.getColumnIndex() == 2) {
							textValue = c.getRichStringCellValue().getString();
							sourceTableDetail.setSrcDbConnectionNm(textValue);
						} else if (c.getColumnIndex() == 4) {
							textValue = c.getRichStringCellValue().getString();
							processMaster.setProcFreqCd(textValue);
						} else if (c.getColumnIndex() == 6) {
							textValue = c.getRichStringCellValue().getString();
							processMaster.setProcId(textValue);
							loadProcess.setProcId(textValue);
							sourceTableDetail.setProcId(textValue);
						} else if (c.getColumnIndex() == 7) {
							textValue = c.getRichStringCellValue().getString();
							processMaster.setProcNm(textValue);
						} else if (c.getColumnIndex() == 9) {
							textValue = c.getRichStringCellValue().getString();
							processMaster.setProcDesc(textValue);
						} else if (c.getColumnIndex() == 10) {
							textValue = c.getRichStringCellValue().getString();
							sourceTableDetail.setSrcSchemaNm(textValue);
						} else if (c.getColumnIndex() == 11) {
							textValue = c.getRichStringCellValue().getString();
							sourceTableDetail.setSrcTblNm(textValue);
						} else if (c.getColumnIndex() == 12) {
							textValue = c.getRichStringCellValue().getString();
							loadProcess.setStgDbNm(textValue);
						} else if (c.getColumnIndex() == 13) {
							textValue = c.getRichStringCellValue().getString();
							loadProcess.setStgTblNm(textValue);
						} else if (c.getColumnIndex() == 14) {
							textValue = c.getRichStringCellValue().getString();
							loadProcess.setTgtDbNm(textValue);
						} else if (c.getColumnIndex() == 15) {
							textValue = c.getRichStringCellValue().getString();
							loadProcess.setTgtTblNm(textValue);
						} else if (c.getColumnIndex() == 16) {
							textValue = c.getRichStringCellValue().getString();
							loadProcess.setTgtAplyTypeCd(textValue);
						} else if (c.getColumnIndex() == 18) {
							textValue = c.getRichStringCellValue().getString();
							processMaster.setDeployNodeNm(textValue);
						}
						
						break;
					case NUMERIC:
						if (c.getColumnIndex() == 8) {
							numericValue = Double.valueOf(c.getNumericCellValue()).intValue();
							processMaster.setProcGrpId(numericValue);
						} else if (c.getColumnIndex() == 17) {
							numericValue = Double.valueOf(c.getNumericCellValue()).intValue();
							loadProcess.setErrThreshold(numericValue);
						}
						
						break;
					default:
						break;
					} // End of Switch statement
				} // End inner for loop
				
				processMaster.setLoadProcess(loadProcess);
				processMaster.setSourceTableDetail(sourceTableDetail);
				
				processMastersMap.put(processMaster.getProcId(), processMaster);
				
				lastProcessedRow = rw + 1;
				processedRowCount++;
			} // End outer for loop
		} finally {
			logger.debug(processedRowCount + " rows parsed successfully from Source File Spec Sheet, last processed row number is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
		
		return processMastersMap;
	}
	
	/**
	 * This method is used to parse the Process Specification excel file's
	 * DataLoading_param and ProcDownstream tab.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @return the processMaster objects in an ArrayList
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	public Map<String, ProcessMaster> parseProcessSpecification(String filePath, Map<String, ProcessMaster> processMastersMap) throws EDAGPOIException {
		XSSFWorkbook wb = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Specifications for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int rw = 0;
		int processedRowCount = 0;
		int lastProcessedRow = 0;
		try {
			// Parse the Data Loading Sheet
			XSSFSheet sheet = wb.getSheet(UobConstants.DATALOADING_PARAM);
			if (sheet == null) {
				throw new EDAGPOIException("There is no \"" + UobConstants.DATALOADING_PARAM + "\" Sheet in the Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.info("Number of Rows in Sheet: " + noOfRows);
			
			String text = null;
			String processId = null;
			
			ProcParam procParam = null;
			ProcDownstreamAppl procDownstreamAppl = null;
			ProcessMaster processMaster = null;
			
			List<ProcParam> procParamsList = new ArrayList<ProcParam>();
			List<ProcDownstreamAppl> procApplList = new ArrayList<ProcDownstreamAppl>();
			
			Cell cel = null;
			for (Row row: sheet) {
				rw = row.getRowNum();
				cel = row.getCell(0);
				
				// Traversing through empty or header rows.
				if (cel != null) {
					switch (cel.getCellTypeEnum()) {
					case STRING:
						text = cel.getRichStringCellValue().getString();
						if (text.trim().equalsIgnoreCase(UobConstants.PROCESS_ID_STR)) {
							continue; // header row
						}
						
						break;
					default:
						break;
					}
				}
				
				procParam = new ProcParam();
				for (Cell c : row) {
					switch (c.getCellTypeEnum()) {
					case STRING:
						if (c.getColumnIndex() == 0) {
							text = c.getRichStringCellValue().getString();
							if (processId == null) {
								processId = text.trim();
								procParam.setProcId(processId);
								
								processMaster = processMastersMap.get(processId);
								processMastersMap.remove(processId);
							} else if (processId.equalsIgnoreCase(text.trim())) {
								procParam.setProcId(processId);
							} else {
								break;
							}
							
						} else if (c.getColumnIndex() == 1) {
							text = c.getRichStringCellValue().getString();
							procParam.setParamNm(text);
						} else if (c.getColumnIndex() == 2) {
							text = c.getRichStringCellValue().getString();
							procParam.setParamVal(text);
						}
						
						break;
					default:
						break;
					} // End Switch
				} // End inner loop
				
				if (StringUtils.isNotBlank(procParam.getProcId())) {
					procParamsList.add(procParam);
				}
			} // End outer loop
			
			processMaster.setProcParamsList(procParamsList);
			
			// Repeat the same thing for ProcDownstreamAppl table.
			for (Row row: sheet) {
				rw = row.getRowNum();
				cel = row.getCell(0);
				
				// Traversing through empty or header rows.
				if (cel != null) {
					switch (cel.getCellTypeEnum()) {
					case STRING:
						text = cel.getRichStringCellValue().getString();
						if (text.trim().equalsIgnoreCase(UobConstants.PROCESS_ID_STR)) {
							continue; // header row
						}
						
						break;
					default:
						break;
					}
				}
				
				procDownstreamAppl = new ProcDownstreamAppl();
				for (Cell c: row) {
					switch (c.getCellTypeEnum()) {
					case STRING:
						if (c.getColumnIndex() == 0) {
							text = c.getRichStringCellValue().getString();
							if (processId.equalsIgnoreCase(text.trim())) {
								procDownstreamAppl.setProcId(processId);
							} else {
								break;
							}
						} else if (c.getColumnIndex() == 1) {
							text = c.getRichStringCellValue().getString();
							procDownstreamAppl.setApplCd(text.trim());
						}
						
						break;
					default:
						break;
					} // End Switch
				} // End inner loop
				
				if (StringUtils.isNotBlank(procParam.getProcId())) {
					procApplList.add(procDownstreamAppl);
				}
			} // End outer loop
			
			processMaster.setProcApplList(procApplList);
			
			processMastersMap.put(processId, processMaster);
			
			processedRowCount++;
			lastProcessedRow = rw + 1;
		} finally {
			logger.debug(processedRowCount + " rows parsed successfully from Source File Spec Sheet, last processed row no is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}		
		
		return processMastersMap;
	}

	/**
	 * This method is used the parse the File Spec sheet in the Interface
	 * Specification.
	 * 
	 * @param filePath
	 *            The file path of the Interface Specification
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the File Spec sheet
	 */
	private InterfaceSpec parseSourceFileSpecification(String filePath) throws EDAGPOIException, EDAGValidationException {
		InterfaceSpec interfaceSpec = new InterfaceSpec();
		XSSFWorkbook wb = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Source File Info for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int row = 0;
		int col = 0;
		int lastProcessedRow = 0;
		int processedRowCount = 0;
		try {
			XSSFSheet sheet = wb.getSheet(UobConstants.UOB_SRC_SYS_SPEC);
			if (sheet == null) {
				throw new EDAGPOIException("There is no \"File Spec\" Sheet in the Interface Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				if (row < 3) {
					continue; // Blank and Header Lines
				} else {
					XSSFRow xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.debug("Skipping Empty Row: " + row);
						continue;
					}
				}

				int serNo = POIUtils.getCellContent(sheet, row, col++, Integer.class);
				String srcFileNm = POIUtils.getCellContent(sheet, row, col++, String.class);

				// Handling scenario where due to some reason row is not null
				// but it is actually a blank line
				if (StringUtils.isBlank(srcFileNm)) {
					logger.debug("Skipping Empty Row: " + row);
					continue;
				}
				
				if (interfaceSpec.getSrcFileSpec() != null && interfaceSpec.getSrcFileSpec().containsKey(srcFileNm)) {
					throw new EDAGPOIException("Duplicate Source Specifications for Source File: " + srcFileNm);
				}

				String srcFileDesc = POIUtils.getCellContent(sheet, row, col++, String.class);
				String srcFileFreq = POIUtils.getCellContent(sheet, row, col++, String.class);
				String srcFileType = POIUtils.getCellContent(sheet, row, col++, String.class);
				String loadType = POIUtils.getCellContent(sheet, row, col++, String.class);
				String srcFileLayout = POIUtils.getCellContent(sheet, row, col++, String.class);
				POIUtils.getCellContent(sheet, row, col++, String.class); // skipping primary key
				POIUtils.getCellContent(sheet, row, col++, String.class); // skipping business key
				POIUtils.getCellContent(sheet, row, col++, String.class); // skipping foreign key
				POIUtils.getCellContent(sheet, row, col++, String.class); // skipping related file
				POIUtils.getCellContent(sheet, row, col++, String.class); // skipping related file key
				String decimalInd = POIUtils.getCellContent(sheet, row, col++, String.class);
				String charset = POIUtils.getCellContent(sheet, row, col++, String.class);
				
				Map<String, CountryAttributes> countryAttributesMap = new HashMap<String, CountryAttributes>();
				for (String countryCode : UobConstants.COUNTRY_CODES) {
					countryAttributesMap.put(countryCode, extractCharsetByCountry(countryCode, sheet, row, col++));
				}
				
				// EDF-203
				for (String countryCode : UobConstants.COUNTRY_CODES) {
					String ref = StringUtils.trimToNull(POIUtils.getCellContent(sheet, row, col++, String.class));
					if (ref != null) {
						CountryAttributes attrs = countryAttributesMap.get(countryCode);
						if (attrs == null) {
							attrs = new CountryAttributes();
							attrs.countryCode = countryCode;
							countryAttributesMap.put(countryCode, attrs);
						}
						
						attrs.referencedFileSourceFolder = ref;
					}
				}

				InterfaceSpecMap rowData = new DefaultInterfaceSpecMap();
				rowData.put(UobConstants.SNO, serNo);
				rowData.put(UobConstants.FILE_NM, srcFileNm);
				rowData.put(UobConstants.FILE_DESC, srcFileDesc);
				rowData.put(UobConstants.FILE_FREQ, srcFileFreq);
				rowData.put(UobConstants.FILE_TYPE, srcFileType);
				rowData.put(UobConstants.LOAD_STRTGY, loadType);
				rowData.put(UobConstants.FILE_FORMAT, srcFileLayout);
				rowData.put(UobConstants.DEC_IND, decimalInd);
				rowData.put(UobConstants.CHARSET, charset);

				for (CountryAttributes attrs : countryAttributesMap.values()) {
					putCountryAttributesInInterfaceSpec(attrs, rowData);
				}

				if ((StringUtils.trimToEmpty(srcFileDesc).equalsIgnoreCase(UobConstants.CONTROL_FILE_PATTERN)) || 
						(StringUtils.trimToEmpty(srcFileType).equalsIgnoreCase(UobConstants.CONTROL_FILE_PATTERN))) {
					if (interfaceSpec.getCtrlFileSpec() != null	&& interfaceSpec.getCtrlFileSpec().containsKey(srcFileNm)) {
						throw new EDAGPOIException("Duplicate Source Specifications for Source File:" + srcFileNm);
					}
					
					interfaceSpec.setCtrlFileSpec(rowData);
				} else {
					if (interfaceSpec.getSrcFileSpec() == null) {
						Map<String, InterfaceSpecMap> srcFileSpec = new HashMap<String, InterfaceSpecMap>();
						srcFileSpec.put(srcFileNm, rowData);
						interfaceSpec.setSrcFileSpec(srcFileSpec);
					} else {
						interfaceSpec.getSrcFileSpec().put(srcFileNm, rowData);
					}
				}
				
				lastProcessedRow = row + 1;
				processedRowCount++;
			}
		} finally {
			logger.debug(processedRowCount +  " rows processed successfully from Source File Spec Sheet, last processed row number is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
		
		return interfaceSpec;
	}

	private void putCountryAttributesInInterfaceSpec(CountryAttributes attrs, InterfaceSpecMap rowData) {
		rowData.put(UobConstants.getCountryInterfaceSpecKey(attrs.countryCode), attrs.supported);
		rowData.put(UobConstants.getCharsetInterfaceSpecKey(attrs.countryCode), attrs.charset);
		rowData.put(UobConstants.getEncodingInterfaceSpecKey(attrs.countryCode), attrs.encoding);
		
		// EDF-203
		if (attrs.referencedFileSourceFolder != null) {
			rowData.put(UobConstants.getReferencedFileFolderSpecKey(attrs.countryCode), attrs.referencedFileSourceFolder);
		}
	}

	private CountryAttributes extractCharsetByCountry(String ctryCode, XSSFSheet sheet, int row, int col) throws EDAGValidationException, EDAGPOIException {
		CountryAttributes attrs = new CountryAttributes();
		
		attrs.countryCode = ctryCode;
		
		boolean customPair = false;
		try {
			attrs.supported = POIUtils.getCellContent(sheet, row, col, Boolean.class);
		} catch (EDAGValidationException e) {
			logger.debug("The previous error indicates a non-boolean value, checking custom charset/encoding pair");
			attrs.supported = true; // a non-boolean value according to UobUtils.parseBoolean() is deemed to be a charset/encoding pair
			customPair = true;
		}
		
		if (attrs.supported && customPair) {
			String value = POIUtils.getCellContent(sheet, row, col, String.class);
			logger.info("Country " + ctryCode + " uses " + value + " as custom charset/encoding pair");
			String[] customValues = value.split("/", -1);
			if (customValues.length != 2) {
				throw new EDAGValidationException(value + " is not a valid charset/encoding pair. " +
			                                    "Sample of valid pairs: 'ISO-8859-1/Big5', 'default/GB2312', 'ISO-8859-1/default', 'default/default'");
			}
			
			String charset = customValues[0].trim();
			String encoding = customValues[1].trim();
			
			if (!VALID_CHARSETS.contains(charset)) {
				throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, charset, "Valid charsets are " + VALID_CHARSETS);
			}
			
			if (!VALID_ENCODINGS.contains(encoding)) {
				throw new EDAGValidationException(EDAGValidationException.INVALID_VALUE, encoding, "Valid encodings are " + VALID_ENCODINGS);
			}
			
			attrs.charset = charset;
			attrs.encoding = encoding;
		}
		
		return attrs;
	}

	/**
	 * This method is used the parse the Field Spec sheets in the Interface
	 * Specification.
	 * 
	 * @param filePath
	 *            The file path of the Interface Specification
	 * @param spec
	 *            The Interface Specification object
	 * @return The parsed source field specifications
	 * @throws Exception
	 *             when there is an error parsing the Field Spec sheet
	 */
	private Map<String, Map<String, InterfaceSpecMap>> parseSourceFieldSpecification(String filePath,
			                                                                             InterfaceSpec spec) throws EDAGPOIException, EDAGValidationException {
		XSSFWorkbook wb = null;
		int row = 0;
		int col = 0;
		Map<String, Map<String, InterfaceSpecMap>> srcFieldSpecInfo = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		try {
			logger.info(filePath + " has " + wb.getNumberOfSheets() + " sheets");
			for (int i = 0; i < wb.getNumberOfSheets(); i++) {
				logger.info("Sheet " + i + " is " + wb.getSheetName(i));
			}
			
			Map<String, InterfaceSpecMap> srcFileSpec = spec.getSrcFileSpec();
			for (String fileName : srcFileSpec.keySet()) {
				logger.info("Going to parse Field info for sheet: " + fileName);
				XSSFSheet sheet = wb.getSheet(fileName);
				if (sheet == null) {
					throw new EDAGPOIException("There is no Field Specification for File: " + fileName);
				}
				
				int noOfRows = getNumberOfRows(sheet);
				logger.debug("Number of Rows in Sheet: " + noOfRows);
				
				int fieldNo = 1;
				int runningPosition = 0;
				RecordType prevRcrdType = null;
				
				int processedRowCount = 0;
				int lastProcessedRow = 0;
				for (row = 0; row <= noOfRows; row++) {
					col = 0;

					if (srcFieldSpecInfo == null) {
						srcFieldSpecInfo = new HashMap<String, Map<String, InterfaceSpecMap>>();
					}

					Object serNoObj = POIUtils.getCellContent(sheet, row, col++, String.class);
					String serNo = null;
					if (serNoObj != null) {
						serNo = (String) serNoObj;
					}
					
					if (StringUtils.isBlank(serNo) || UobConstants.SNO_PATTERN.equalsIgnoreCase(serNo)) {
						continue; // Blank Line or Header
					}

					InterfaceSpecMap rowData = new DefaultInterfaceSpecMap();
					String fieldName = POIUtils.getCellContent(sheet, row, col++, String.class);
					rowData.put(UobConstants.FIELD_NM, fieldName);
					String fieldType = POIUtils.getCellContent(sheet, row, col++, String.class);
					rowData.put(UobConstants.FIELD_TYPE, fieldType);
					int fieldLen = POIUtils.getCellContent(sheet, row, col++, Integer.class);
					rowData.put(UobConstants.FIELD_LEN, fieldLen);
					int decimalPrec = POIUtils.getCellContent(sheet, row, col++, Integer.class);
					rowData.put(UobConstants.DEC_PREC, decimalPrec);
					RecordType rcrdType = RecordType.fromString(POIUtils.getCellContent(sheet, row, col++, String.class));

					if ((prevRcrdType != null && prevRcrdType.isHeader() && !rcrdType.isHeader()) ||
							((prevRcrdType == null || (!prevRcrdType.isHeader() && !prevRcrdType.isFooter())) && rcrdType.isFooter())) {
						// Data Rows Start, Restart Field Nums
						fieldNo = 1;
						runningPosition = 0; // Added by Ganapathy
					} 
					
					prevRcrdType = rcrdType;
					int fieldNum = fieldNo++;
					String mandatory = POIUtils.getCellContent(sheet, row, col++, String.class);
					String fieldDesc = POIUtils.getCellContent(sheet, row, col++, String.class);
					String format = POIUtils.getCellContent(sheet, row, col++, String.class);
					String defaultValue = POIUtils.getCellContent(sheet, row, col++, String.class);
					String remarks = POIUtils.getCellContent(sheet, row, col++, String.class);
					String pii = POIUtils.getCellContent(sheet, row, col++, String.class);
					String excelHeader = POIUtils.getCellContent(sheet, row, col++, String.class);
					String regexExpression = POIUtils.getCellContent(sheet, row, col++, String.class);
					
					rowData.put(UobConstants.SNO, serNo);
					rowData.put(UobConstants.FIELD_NUM, fieldNum);
					rowData.put(UobConstants.RCRD_TYPE, rcrdType);
					rowData.put(UobConstants.MDT_OPT, mandatory);
					rowData.put(UobConstants.FIELD_DESC, fieldDesc);
					rowData.put(UobConstants.FORMAT, format);
					rowData.put(UobConstants.DEFAULT_VAL, defaultValue);
					rowData.put(UobConstants.REMARKS, remarks);
					rowData.put(UobConstants.PII, pii);
					rowData.put(UobConstants.EXCEL_FIELD_HEADER_MAPPING, excelHeader);
					rowData.put(UobConstants.REGULAR_EXPRESSION, regexExpression);

//	                if (sheet.getSheetName().equalsIgnoreCase("PSERVER_LOG")||sheet.getSheetName().equalsIgnoreCase("PSERVER_LOG2")) {
//						rowData.put(UobConstants.REGULAR_EXPRESSION, POIUtils.getCellContent(sheet, row, col++, String.class));
//					}
//					//Excel Header Field Mapping, if the column comes after remarks 
//					String headerField = POIUtils.getCellContent(sheet, headerRow, col, String.class);
//					if(headerField.toLowerCase().contains("excel") || headerField.toLowerCase().contains("mapping")) {
//						rowData.put(UobConstants.EXCEL_FIELD_HEADER_MAPPING, POIUtils.getCellContent(sheet, row, col++, String.class));
//					}
					
					// the additional 17 columns
					rowData.put(UobConstants.BIZ_TERM, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.BIZ_DEFINITION, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SYNONYMS, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.USAGE_CONTEXT, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SYSTEM_STEWARD, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_SYSTEM, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_TABLE, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_FIELD_NAME, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_FIELD_DESC, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_FIELD_TYPE, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_FIELD_LENGTH, POIUtils.getCellContent(sheet, row, col++, Integer.class));
					rowData.put(UobConstants.SOURCE_FIELD_FORMAT, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SOURCE_DATA_CATEGORY, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.LOV_CODE_AND_DESC, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.OPTIONALITY_2, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.SYSDATA_VALIDATION_LOGIC, POIUtils.getCellContent(sheet, row, col++, String.class));
					rowData.put(UobConstants.DATA_AVAILABILITY, POIUtils.getCellContent(sheet, row, col++, String.class));
					
					//Excel Header Field Mapping if the column comes after optional columns
//					headerField = POIUtils.getCellContent(sheet, row - 1, col + 1, String.class);
//					if(headerField.toLowerCase().contains("excel") || headerField.toLowerCase().contains("mapping")) {
//						// the additional 17 columns
//						rowData.put(UobConstants.EXCEL_FIELD_HEADER_MAPPING, POIUtils.getCellContent(sheet, row, col++, String.class));
//					}
                                        
					int startPos = runningPosition;
					runningPosition = runningPosition + fieldLen; 
					int endPos = runningPosition;

					logger.debug("Start Position " + startPos + ", end position " + endPos);

					rowData.put(UobConstants.FIELD_START_POS, startPos);
					rowData.put(UobConstants.FIELD_END_POS, endPos);

					if (srcFieldSpecInfo.containsKey(fileName)) {
						Map<String, InterfaceSpecMap> rowListData = srcFieldSpecInfo.get(fileName);
						String key = fieldName + UobConstants.UNDERSCORE + rcrdType;
						if (rowListData.containsKey(key)) {
							throw new EDAGPOIException("Duplicate Field Name: " + fieldName + ", Record Type: " + rcrdType + 
									                        " on file: " + fileName);
						} else {
							rowListData.put(key, rowData);
						}
						
						srcFieldSpecInfo.put(fileName, rowListData);
					} else {
						Map<String, InterfaceSpecMap> rowListData = new HashMap<String, InterfaceSpecMap>();
						String key = fieldName + UobConstants.UNDERSCORE + rcrdType;
						rowListData.put(key, rowData);
						srcFieldSpecInfo.put(fileName, rowListData);
					}

					lastProcessedRow = row + 1;
					processedRowCount++;
				}
				
				logger.debug(processedRowCount + " rows parsed successfully from Source Field Spec Sheet, last processed row number is " + lastProcessedRow);
			}
			
			return srcFieldSpecInfo;
		} finally {
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
	}

	/**
	 * This method is used the parse the Control Field Spec sheets in the
	 * Interface Specification.
	 * 
	 * @param filePath
	 *            The file path of the Interface Specification
	 * @param spec
	 *            The Interface Specification object
	 * @return The parsed source field specifications
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the Field Spec sheet
	 */
	private Map<String, InterfaceSpecMap> parseControlFieldSpecification(String filePath, InterfaceSpec spec) throws EDAGPOIException, EDAGValidationException {
		XSSFWorkbook wb = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int row = 0;
		int col = 0;
		int processedRowCount = 0;
		int lastProcessedRow = 0;
		try {
			InterfaceSpecMap ctrlFileSpec = spec.getCtrlFileSpec();
			String fileName = ctrlFileSpec.get(UobConstants.FILE_NM, String.class);
			logger.debug("Going to parse Field info for sheet:" + fileName);
			XSSFSheet sheet = wb.getSheet(fileName);
			if (sheet == null) {
				throw new EDAGPOIException("There is no Field Specification for File: " + fileName);
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			int fieldNo = 1;
			int runningPosition = 0;
			RecordType prevRcrdType = null;
			Map<String, InterfaceSpecMap> rowListData = null;
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				String serNoObj = POIUtils.getCellContent(sheet, row, col++, String.class);
				String serNo = null;
				if (serNoObj != null) {
					serNo = (String) serNoObj;
				}
				
				if (StringUtils.isEmpty(serNo) || UobConstants.SNO_PATTERN.equalsIgnoreCase(serNo)) {
					continue; // Blank Line or Header
				}

				InterfaceSpecMap rowData = new DefaultInterfaceSpecMap();
				String fieldName = POIUtils.getCellContent(sheet, row, col++, String.class);
				rowData.put(UobConstants.FIELD_NM, fieldName);
				String fieldType = POIUtils.getCellContent(sheet, row, col++, String.class);
				rowData.put(UobConstants.FIELD_TYPE, fieldType);
				int fieldLen = POIUtils.getCellContent(sheet, row, col++, Integer.class);
				rowData.put(UobConstants.FIELD_LEN, fieldLen);
				int decimalPrec = POIUtils.getCellContent(sheet, row, col++, Integer.class);
				rowData.put(UobConstants.DEC_PREC, decimalPrec);
				RecordType rcrdType = RecordType.fromString(POIUtils.getCellContent(sheet, row, col++, String.class));
				if (rcrdType != RecordType.HEADER && rcrdType != RecordType.FOOTER) {
					rcrdType = RecordType.CONTROL_INFO;
				}

				if ((prevRcrdType != null && prevRcrdType.isHeader() && !rcrdType.isHeader()) ||
						((prevRcrdType == null || (!prevRcrdType.isHeader() && !prevRcrdType.isFooter())) && rcrdType.isFooter())) {
					// Data Rows / Footer Rows Start, Restart Field Nums
					fieldNo = 1;
				}
				
				prevRcrdType = rcrdType;
				int fieldNum = fieldNo++;
				String mandatory = POIUtils.getCellContent(sheet, row, col++, String.class);
				String fieldDesc = POIUtils.getCellContent(sheet, row, col++, String.class);
				String format = POIUtils.getCellContent(sheet, row, col++, String.class);
				String defaultValue = POIUtils.getCellContent(sheet, row, col++, String.class);
				String remarks = POIUtils.getCellContent(sheet, row, col++, String.class);
				rowData.put(UobConstants.SNO, serNo);
				rowData.put(UobConstants.FIELD_NUM, fieldNum);
				rowData.put(UobConstants.RCRD_TYPE, rcrdType);
				rowData.put(UobConstants.MDT_OPT, mandatory);
				rowData.put(UobConstants.FIELD_DESC, fieldDesc);
				rowData.put(UobConstants.FORMAT, format);
				rowData.put(UobConstants.DEFAULT_VAL, defaultValue);
				rowData.put(UobConstants.REMARKS, remarks);
				// the additional 17 columns
				rowData.put(UobConstants.BIZ_TERM, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.BIZ_DEFINITION, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SYNONYMS, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.USAGE_CONTEXT, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SYSTEM_STEWARD, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_SYSTEM, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_TABLE, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_FIELD_NAME, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_FIELD_DESC, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_FIELD_TYPE, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_FIELD_LENGTH, POIUtils.getCellContent(sheet, row, col++, Integer.class));
				rowData.put(UobConstants.SOURCE_FIELD_FORMAT, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SOURCE_DATA_CATEGORY, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.LOV_CODE_AND_DESC, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.OPTIONALITY_2, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.SYSDATA_VALIDATION_LOGIC, POIUtils.getCellContent(sheet, row, col++, String.class));
				rowData.put(UobConstants.DATA_AVAILABILITY, POIUtils.getCellContent(sheet, row, col++, String.class));

				int startPos = runningPosition;
				runningPosition = runningPosition + fieldLen; 
				int endPos = runningPosition;

				rowData.put(UobConstants.FIELD_START_POS, startPos);
				rowData.put(UobConstants.FIELD_END_POS, endPos);

				String key = fieldName + UobConstants.UNDERSCORE + rcrdType;
				if (rowListData == null) {
					rowListData = new HashMap<String, InterfaceSpecMap>();
				}
				
				if (rowListData.containsKey(key)) {
					throw new EDAGPOIException("Duplicate Field Name: " + fieldName + ", Record Type: " + rcrdType + 
							                        " on file: " + fileName);
				} else {
					rowListData.put(key, rowData);
				}

				processedRowCount++;
				lastProcessedRow = row + 1;
			}
			
			return rowListData;
		} finally {
			logger.debug(processedRowCount + " rows parsed successfully from Source Field Spec Sheet, last processed row number is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
	}

	/**
	 * This method is used to get the number of rows in a given excel sheet.
	 * 
	 * @param sheet
	 *            the Excel Sheet object
	 * @return the number of rows
	 * @throws Exception
	 *             when there is an error retrieving the number of rows
	 */
	private int getNumberOfRows(Sheet sheet) {
		return sheet.getLastRowNum();
	}

	/**
	 * This method is used to parse the Specification File Inputs.
	 * 
	 * @param procSpecPath
	 *            The Path of the Process Specification file on the Server
	 * @return the Interface Specification object
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is any error parsing the process specification
	 */
	public InterfaceSpec parseExportProcessSpecFile(String procSpecPath) throws EDAGPOIException, EDAGValidationException {
		logger.info("Parsing Process Spec File:" + procSpecPath);
		InterfaceSpec interSpec = null;
		Map<String, InterfaceSpecMap> processSpec = parseExportProcSpecSheet(procSpecPath);

		interSpec = new InterfaceSpec();
		interSpec.setExportProcessSpec(processSpec);

		Map<String, Map<String, String>> processParamSpec = parseExportProcessParamSpecification(procSpecPath);
		interSpec.setParamSpec(processParamSpec);
		
		Map<String, String> processDownstreamSpec = parseProcessDownstreamSpecification(procSpecPath);
		interSpec.setDownstreamSpec(processDownstreamSpec);
		
		logger.debug("Parsed Spec is: " + interSpec);
		
		return interSpec;
	}

	/**
	 * This method is used to parse the Process Specification excel file.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @return the parsed values in a HashMap
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	private Map<String, InterfaceSpecMap> parseExportProcSpecSheet(String filePath) throws EDAGPOIException, EDAGValidationException {
		XSSFWorkbook wb = null;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Specifications for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int row = 0;
		int col = 0;
		Map<String, InterfaceSpecMap> procSpecInfo = null;
		int processedRowCount = 0;
		int lastProcessedRow = 0;
		try {
			// Parse the Data Ingestion Sheet
			XSSFSheet sheet = wb.getSheet(UobConstants.DATA_EXPORT_SPEC);
			if (sheet == null) {
				throw new EDAGPOIException("There is no \"DataExport\" Sheet in the Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				if (row < 1) {
					continue; // Header Line
				} else {
					XSSFRow xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.debug("Skipping Empty Row: " + row);
						continue;
					}
				}
				
				if (procSpecInfo == null) {
					procSpecInfo = new HashMap<String, InterfaceSpecMap>();
				}

				String serNo = POIUtils.getCellContent(sheet, row, col++, String.class);

				if (StringUtils.isBlank(serNo)) { // TODO
					logger.debug("Skipping Empty Row: " + row);
					continue;
				}

				String srcSysName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String procId = POIUtils.getCellContent(sheet, row, col++, String.class);
				String procName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String procGroup = POIUtils.getCellContent(sheet, row, col++, String.class);
				String countryCode = POIUtils.getCellContent(sheet, row, col++, String.class);
				String frequency = POIUtils.getCellContent(sheet, row, col++, String.class);
				String srcDbName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String srcTblName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String tgtDirName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String tgtFileName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String tgtFileExtn = POIUtils.getCellContent(sheet, row, col++, String.class);
				String tgtColDelim = POIUtils.getCellContent(sheet, row, col++, String.class);
				String tgtTxtDelim = POIUtils.getCellContent(sheet, row, col++, String.class);
				String ctrlFileNm = POIUtils.getCellContent(sheet, row, col++, String.class);
				String ctrlFileExtn = POIUtils.getCellContent(sheet, row, col++, String.class);
				String deployNode = POIUtils.getCellContent(sheet, row, col++, String.class);
				String alertEmail = POIUtils.getCellContent(sheet, row, col++, String.class);

				InterfaceSpecMap rowData = new DefaultInterfaceSpecMap();
				rowData.put(UobConstants.SNO, serNo);
				rowData.put(UobConstants.SRC_SYS_NM, srcSysName);
				rowData.put(UobConstants.PROCESS_ID, procId);
				rowData.put(UobConstants.PROCESS_NAME, procName);
				rowData.put(UobConstants.PROCESS_GRP, procGroup);
				rowData.put(UobConstants.CTRY_CD, countryCode);
				rowData.put(UobConstants.FREQ_CD, frequency);
				rowData.put(UobConstants.SRC_DB_NAME, srcDbName);
				rowData.put(UobConstants.SRC_TBL_NAME, srcTblName);
				rowData.put(UobConstants.TGT_DIR_NAME, tgtDirName);
				rowData.put(UobConstants.TGT_FILE_NAME, tgtFileName);
				rowData.put(UobConstants.TGT_FILE_EXTN, tgtFileExtn);
				rowData.put(UobConstants.TGT_COL_DELIM, tgtColDelim);
				rowData.put(UobConstants.TGT_TXT_DELIM, tgtTxtDelim);
				rowData.put(UobConstants.CTRL_FILE_NM, ctrlFileNm);
				rowData.put(UobConstants.CTRL_FILE_EXTN, ctrlFileExtn);
				rowData.put(UobConstants.DEPLOYMENT_NODE, deployNode);
				rowData.put(UobConstants.ALERT_EMAIL, alertEmail);

				if (procSpecInfo.containsKey(procId)) {
					throw new EDAGPOIException("Duplicate Process Specifications for Process ID: " + procId);
				}

				procSpecInfo.put(procId, rowData);
				processedRowCount++;
				lastProcessedRow = row + 1;
			}
			
			return procSpecInfo;
		} finally {
			logger.debug(processedRowCount + " rows parsed successfully from Process Spec Sheet, last processed row number is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
	}

	/**
	 * This method is used to parse the Process Specification Params excel sheet
	 * for Export.
	 * 
	 * @param filePath
	 *            The File Path of the Process Specification
	 * @return the parsed values in a HashMap
	 * @throws EDAGValidationException 
	 * @throws Exception
	 *             when there is an error parsing the excel
	 */
	private Map<String, Map<String, String>> parseExportProcessParamSpecification(String filePath) throws EDAGPOIException, EDAGValidationException {
		Workbook wb;
		
		try {
			wb = new XSSFWorkbook(new FileInputStream(filePath));
			logger.info("Going to parse Process Param Specifications Params for file: " + filePath);
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, filePath, e.getMessage());
		}
		
		int row = 0;
		int col = 0;
		Map<String, Map<String, String>> procSpecInfo = null;
		int processedRowCount = 0;
		int lastProcessedRow = 0;
		try {
			Sheet sheet = wb.getSheet(UobConstants.DATA_EXPORT_PARAM_SPEC);
			if (sheet == null) {
				throw new EDAGPOIException("There is no Data Export Param Sheet in Process Specification");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			for (row = 0; row <= noOfRows; row++) {
				col = 0;
				if (row < 1) {
					continue; // Header Line
				} else {
					Row xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.debug("Skipping Empty Row: " + row);
						continue;
					}
				}
				
				if (procSpecInfo == null) {
					procSpecInfo = new HashMap<String, Map<String, String>>();
				}

				String procId = POIUtils.getCellContent(sheet, row, col++, String.class);

				if (StringUtils.isEmpty(procId)) {
					continue; // Skip the row
				}
				
				String parameterName = POIUtils.getCellContent(sheet, row, col++, String.class);
				String parameterValue = POIUtils.getCellContent(sheet, row, col++, String.class);

				Map<String, String> rowData = new HashMap<String, String>();
				rowData.put(parameterName, parameterValue);

				procSpecInfo.put(procId, rowData);

				lastProcessedRow = row + 1;
				processedRowCount++;
			}
			
			return procSpecInfo;
		} finally {
			logger.debug(processedRowCount + " rows parsed successfully from Process Param Spec Sheet, last processed row number is " + lastProcessedRow);
			
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + filePath + ": " + e.getMessage());
			}
		}
	}
	
	public List<LobStampingInfo> parseLobRegistrationFile(String registrationFilePath) throws EDAGPOIException, EDAGValidationException {
		List<LobStampingInfo> lobStampingInfos = new ArrayList<>();
		XSSFWorkbook wb = null;
		try {
			wb = new XSSFWorkbook(new FileInputStream(registrationFilePath));
		} catch (IOException e) {
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, registrationFilePath, e.getMessage());
		}
		try {
			XSSFSheet sheet = wb.getSheetAt(0);
			if (sheet == null) {
				throw new EDAGPOIException("There is no Sheet for LOB Stamping SQL registration");
			}
			
			int noOfRows = getNumberOfRows(sheet);
			logger.debug("Number of Rows in Sheet: " + noOfRows);
			for (int row = 0; row <= noOfRows; row++) {
				int col = 0;
				if (row < 1) {
					continue; // Header Line
				} else {
					XSSFRow xssfRow = sheet.getRow(row);
					if (xssfRow == null) {
						logger.debug("Skipping Empty Row: " + row);
						continue;
					}
				}
				
				String processId = POIUtils.getCellContent(sheet, row, col++, String.class);
				String dependantTables = POIUtils.getCellContent(sheet, row, col++, String.class);
				String lobStampingSQL = POIUtils.getCellContent(sheet, row, col++, String.class);
				String siteIdComputeSQL = POIUtils.getCellContent(sheet, row, col++, String.class);
				String hiveTableName = POIUtils.getCellContent(sheet, row, col++, String.class);
				LobStampingInfo lobStampingInfo = new LobStampingInfo(processId, dependantTables, lobStampingSQL, siteIdComputeSQL, hiveTableName);
				lobStampingInfos.add(lobStampingInfo);
			}
		} finally {
			try {
				wb.close();
			} catch (IOException e) {
				logger.warn("Unable to close workbook " + registrationFilePath + ": " + e.getMessage());
			}
		}
		return lobStampingInfos;
	}
}