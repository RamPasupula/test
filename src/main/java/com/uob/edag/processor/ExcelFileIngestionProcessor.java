package com.uob.edag.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.monitorjbl.xlsx.StreamingReader;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.constants.UobConstants.DateConverter;
import com.uob.edag.constants.UobConstants.FileLayoutIdentifier;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGPOIException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.POIUtils;
import com.uob.edag.utils.PropertyLoader;

public class ExcelFileIngestionProcessor extends FileIngestionProcessor implements IngestionProcessor {

	private static final String BUSINESS_DATE_PARAM_DEFAULT_FORMAT = "yyyy-MM-dd";

	private static final String REGULAR_EXPRESSION = "\\s+|\\n|\\r";

	private FileUtility fileUtils;

	private int headerFieldCount = 0;
	
	private int dynamicHeaderFieldCount = 0;
	
	private int staticHeaderFieldCount = 0;
	
	private int noOfYears = 0;
	
	private int startYearCount = 0;

	private List<String> fieldNames = new ArrayList<>();

	/**
	 * This method is used to run the Ingestion Process for file based sources.
	 * 
	 * @param procInstanceModel
	 *            The process Instance Model object
	 * @param processModel
	 *            The Process Model object of the Ingestion Process
	 * @param bizDate
	 *            The business date of the process
	 * @param ctryCd
	 *            The country code of the process
	 * @param forceRerun
	 *            Indicator to show if the Ingestion has to be force rerun from the start
	 * @param forceFileName
	 *            Name of the file which has to force ingested
	 * @throws Exception
	 *             when there is an error in the file Ingestion process
	 */
	public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel processModel, String bizDate, String ctryCd,
			boolean forceRerun, String forceFileName) throws EDAGException {

		logger.info("Going to run Excel File Ingestion for process: " + processModel.getProcId());
		this.fileUtils = FileUtilityFactory.getFileUtility(processModel);
		List<ProcessParam> processParamList = processModel.getProcParam();
		String excelFileName = "";
		String excelSheetName = "";
		String businessDateFormat = UobConstants.EXCEL_BUSINESS_DATE_DEFAULT_FORMAT;
		for (ProcessParam param : processParamList) {
			if (UobConstants.EXCEL_FILE_NAME.equalsIgnoreCase(param.getParamName())) {
				excelFileName = param.getParamValue();
			} else if (UobConstants.EXCEL_SHEET_NAME.equalsIgnoreCase(param.getParamName())) {
				excelSheetName = param.getParamValue();
			} else if (UobConstants.EXCEL_BUSINESS_DATE_FORMAT.equalsIgnoreCase(param.getParamName())) {
				businessDateFormat = param.getParamValue();
			}
		}

		logger.info(String.format("Excel File Name is  %s; Excel Sheet Name is %s", excelFileName, excelSheetName));

		if (StringUtils.isBlank(excelFileName)) {
			throw new EDAGProcessorException(EDAGProcessorException.MISSING_EXCEL_FILE_DEFINITION, processModel.getProcId());
		}

		if (excelFileName.indexOf(String.format("%s{%s}", UobConstants.DOLLAR, UobConstants.EXCEL_BUSINESS_DATE_PARAM_NAME)) != -1) {
			try {
				SimpleDateFormat sourceSDF = new SimpleDateFormat(BUSINESS_DATE_PARAM_DEFAULT_FORMAT);
				SimpleDateFormat destinationSDF = new SimpleDateFormat(businessDateFormat);
				excelFileName = excelFileName.replace(
						String.format("%s{%s}", UobConstants.DOLLAR, UobConstants.EXCEL_BUSINESS_DATE_PARAM_NAME),
						destinationSDF.format(sourceSDF.parse(bizDate)));
			} catch (ParseException e) {
				throw new EDAGException(
						String.format("Cannot parse biz date %s with format %s", bizDate, BUSINESS_DATE_PARAM_DEFAULT_FORMAT));
			}
		}

		Map<String, List<String>> fieldNamePatterns = ingestDao.getFieldNamePatterns(processModel.getProcId());

		FileModel fileModel = processModel.getSrcInfo();
		String completeFilePath = fileModel.getSourceDirectory().substring(0,
				fileModel.getSourceDirectory().indexOf(fileModel.getSourceFileName()));
		String fileName = String.format("%s%s", completeFilePath, excelFileName);

		// EDF 236
		procInstanceModel = BaseProcessor.setFileSizeTime(fileName, procInstanceModel); // EDF 236
		logger.info(String.format("Re-setting the file size for Excel file [%s]:%s", fileName, procInstanceModel.getSrcFileSizeBytes()));

		ingestDao.updateProcessLogFileSizeTime(procInstanceModel); // EDF 236
		logger.info("Updated Excel File Attributes = { " + "file size (bytes): " + procInstanceModel.getSrcFileSizeBytes()
				+ ", file arrival time: " + procInstanceModel.getSrcFileArrivalTime() + " }");

		// End of EDF 236

		File file = new File(fileName);
		if (!file.exists()) {
			throw new EDAGProcessorException(EDAGProcessorException.MISSING_EXCEL_FILE, fileName, processModel.getProcId());
		}

		// archive file from landing location to previous folder
		logger.info(String.format("Archiving the excel file %s", excelFileName));
		String archiveFilePath = String.format("%s%s.%s",
				fileModel.getSourceArchivalDir().substring(0, fileModel.getSourceArchivalDir().indexOf(fileModel.getSourceFileName())),
				excelFileName, UobConstants.BIZ_DATE_PARAM);
		archiveFilePath = archiveFilePath.replaceAll(UobConstants.BIZ_DATE_PARAM, procInstanceModel.getBizDate());
		logger.info("archiveFilePath " + archiveFilePath);
		logger.info("fileName " + fileName);
		this.fileUtils.archiveFile(fileName, archiveFilePath, false);

		// Retrieving the charset
		CountryAttributes attrs = processModel.getCountryAttributesMap().get(procInstanceModel.getCountryCd());
		String charset = attrs != null ? attrs.getCharset(true) : PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET);

		// Copy the records and create a temporary file for ingestion
		int noOfRecords = readRecordsFromExcelFile(fileName, fileModel, fieldNamePatterns, excelSheetName, charset, processModel.getProcId());
		super.setNoOfRecords(noOfRecords);

		// Run normal ingestion process
		super.runFileIngestion(procInstanceModel, processModel, bizDate, ctryCd, forceRerun, forceFileName);

	}

	private int readRecordsFromExcelFile(String sourcefile, FileModel fileModel, Map<String, List<String>> fieldNamePatterns,
			String excelSheetName, String charset, String procId) throws EDAGException {

		List<String> columnValuesToIgnore = Arrays
				.asList(PropertyLoader.getProperty(UobConstants.EXCEL_COLUMN_VALUES_TO_IGNORE).split(",")).stream()
				.map(a -> StringUtils.trim(a.toLowerCase())).collect(Collectors.toList());

		String regexExpression = PropertyLoader
				.getProperty(UobConstants.EXCEL_HEADER_FIELD_SPECIAL_CHARACTER_REPLACE_REGEX_EXPRESSION);

		if (regexExpression != null) {
			try {
				Pattern.compile(regexExpression);
			} catch (PatternSyntaxException e) {
				logger.error(String.format("Invalid Regex Expression - %s is configured in the configuration file."
						+ "Hence will not use regex expression for special character replacement", regexExpression));
				regexExpression = UobConstants.EMPTY;
			}
		}

		if (StringUtils.trimToNull(regexExpression) == null) {
			regexExpression = UobConstants.EMPTY;
		}
		
		List<String> transposeProcIdList = Arrays.asList(PropertyLoader.getProperty("PROC_ID_LIST_FOR_TRANSPOSE_EXCEL").split(","))
				.stream().map(a -> StringUtils.trim(a.toLowerCase())).collect(Collectors.toList());

		Charset characterSet = charset == null ? Charset.forName(PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET))
				: Charset.forName(charset);
		int noOfRecords = 0;
		try (BufferedWriter writer = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(new File(fileModel.getSourceDirectory()))));
				Workbook workbook = createWorkBookBasedOfFileType(
						FileLayoutIdentifier.getFileLayoutType(fileModel.getSourceFileLayoutCd()), sourcefile)) {
			Sheet sheet = null;
			if (StringUtils.isEmpty(excelSheetName)) {
				sheet = workbook.getSheetAt(0);
			} else {
				sheet = workbook.getSheet(excelSheetName);
			}

			if (sheet != null) {
				FileLayoutIdentifier fileLayout = FileLayoutIdentifier.getFileLayoutType(fileModel.getSourceFileLayoutCd());
				String delimiter = StringEscapeUtils.unescapeJava(fileModel.getColumnDelimiter());

				int noOfRows = sheet.getLastRowNum();
				logger.debug("Number of Rows in Sheet: " + noOfRows);
				boolean foundHeader = Boolean.FALSE;
				boolean isHeaderAvailable = isHeaderAvailable(fileLayout);
				List<String> fieldNames = fileModel.getSrcFieldInfo().stream().map(f -> f.getFieldName()).collect(Collectors.toList());
				
				if (transposeProcIdList.contains(procId.toLowerCase())) {
				List<String> fieldColumnList = fileModel.getSrcFieldInfo().stream().map(mapper -> (String.valueOf(mapper.getFieldNum() - 1)))
						.collect(Collectors.toList());
				Map<Integer, FieldModel> fieldNumToFieldModelMapping = fileModel.getSrcFieldInfo().stream()
						.collect(Collectors.toMap(f -> f.getFieldNum() - 1, f -> f));
				
				//fieldColumnList.sort((c1, c2) -> Integer.compare(c1, c2));
				Iterator<Row> rowIterator = sheet.rowIterator();
				
				//Calculating number of static and dynamic column count
				List<String> tempFieldNames = fileModel.getSrcFieldInfo().stream().map(f -> f.getFieldName()).collect(Collectors.toList());
				for (int i = 0; i < tempFieldNames.size(); i++) {
					String tempFieldName = tempFieldNames.get(i);
					List<String> fieldMappingEntries = fieldNamePatterns.get(tempFieldName);
					if (fieldMappingEntries != null) {
						if(fieldMappingEntries.get(0).contains("{$year}")) 
							this.dynamicHeaderFieldCount ++;
						else
							this.staticHeaderFieldCount ++;
						}
				}
				//end
				
				int expectedNoOfColumns = fileModel.getSrcFieldInfo().size();
				String propName = "EXCEL_TRANSPOSE_START_YEAR_" + procId.split("_")[2];
				startYearCount = Integer.parseInt(PropertyLoader.getProperty(propName));
								
				while (rowIterator.hasNext()) {
					Row row = rowIterator.next();
					if (!checkIfRowIsEmpty(row, fieldColumnList.size())) {
						if (isHeaderAvailable && !foundHeader) {
							if(!checkRowsBeforeHeader(row, fieldColumnList.size()))
								this.noOfYears = (row.getLastCellNum() - this.staticHeaderFieldCount)/this.dynamicHeaderFieldCount + 1;
							if (isHeaderRowForTranspose(row, fieldNamePatterns, fileModel, regexExpression)) {
								fieldColumnList = readHeaderRowForTranspose(row, fieldNamePatterns, fileModel, regexExpression);
								for (int i = 0; i < fieldColumnList.size(); i++) {
									String cellNum = fieldColumnList.get(i);
									if (cellNum == null) {
										throw new EDAGProcessorException(EDAGProcessorException.MISSING_HEADER_FIELD_DEFINITION,
												fieldNumToFieldModelMapping.get(i));
									}
								}
								foundHeader = Boolean.TRUE;
							}
							continue;
						}
						for(int j=0;j<noOfYears;j++)
						{
						StringBuilder sb = new StringBuilder();
						for (int i = 0; i < fieldColumnList.size(); i++) {
							Integer cellNum;
							String value = "";
							if(fieldColumnList.get(i).equalsIgnoreCase("{$year}"))
							{
								value = String.valueOf(startYearCount + j);
							}
							else {
							if(fieldColumnList.get(i).contains("#"))
							{
								cellNum = Integer.parseInt(fieldColumnList.get(i).split("#")[j]);
							}
							else {
								cellNum = Integer.parseInt(fieldColumnList.get(i));
							}
							FieldModel fieldModel = fieldNumToFieldModelMapping.get(i);
							value = POIUtils.getCellContent(row, cellNum, String.class);
							value = value.replaceAll(REGULAR_EXPRESSION, " ");
							if (UobConstants.SRC_SIGNED_DECIMAL.equalsIgnoreCase(fieldModel.getDataType())
									|| UobConstants.SRC_NUMERIC.equalsIgnoreCase(fieldModel.getDataType())) {
								if (columnValuesToIgnore.contains(value.toLowerCase())) {
									value = "";
								}
								if (StringUtils.trimToNull(value) != null) {
									try {
										value = value.replaceAll(UobConstants.COMMA, UobConstants.EMPTY);
										new BigDecimal(value);
									} catch (NumberFormatException nfe) {

									}
								}

							} else if (UobConstants.SRC_TIMESTAMP.equalsIgnoreCase(fieldModel.getDataType())) {
								SimpleDateFormat sdf = new SimpleDateFormat(
										DateConverter.getEquivalentJavaFormat(fieldModel.getDataFormat()));
								try {
									value = sdf.format(new Date(Long.valueOf(value)));
								} catch (NumberFormatException nfe) {
									try {
										sdf.parse(value);
									} catch (ParseException e) {
										value = UobConstants.EMPTY;
									}
								}
							}
							}
							String charsetString = new String(
									new String(value.getBytes(), Charset.forName("UTF-8")).getBytes(characterSet));
							sb.append(charsetString).append(delimiter);
						}
						noOfRecords++;
						writer.write(sb.substring(0, (sb.length() - delimiter.length())));
						writer.newLine();
					}
					}
				}
				}
				else {

					List<Integer> fieldColumnList = fileModel.getSrcFieldInfo().stream().map(mapper -> (mapper.getFieldNum() - 1))
							.collect(Collectors.toList());
					Map<Integer, FieldModel> fieldNumToFieldModelMapping = fileModel.getSrcFieldInfo().stream()
							.collect(Collectors.toMap(f -> f.getFieldNum() - 1, f -> f));
					
					fieldColumnList.sort((c1, c2) -> Integer.compare(c1, c2));
					Iterator<Row> rowIterator = sheet.rowIterator();
					while (rowIterator.hasNext()) {
						Row row = rowIterator.next();
						if (!checkIfRowIsEmpty(row, fieldColumnList.size())) {
							if (isHeaderAvailable && !foundHeader) {
								if (isHeaderRow(row, fieldNamePatterns, fileModel, regexExpression)) {
									fieldColumnList = readHeaderRow(row, fieldNamePatterns, fileModel, regexExpression);
									for (int i = 0; i < fieldColumnList.size(); i++) {
										Integer cellNum = fieldColumnList.get(i);
										if (cellNum == null) {
											throw new EDAGProcessorException(EDAGProcessorException.MISSING_HEADER_FIELD_DEFINITION,
													fieldNumToFieldModelMapping.get(i));
										}
									}
									foundHeader = Boolean.TRUE;
								}
								continue;
							}
							StringBuilder sb = new StringBuilder();
							for (int i = 0; i < fieldColumnList.size(); i++) {
								Integer cellNum = fieldColumnList.get(i);
								FieldModel fieldModel = fieldNumToFieldModelMapping.get(i);
								String value = POIUtils.getCellContent(row, cellNum, String.class);
								value = value.replaceAll(REGULAR_EXPRESSION, " ");
								if (UobConstants.SRC_SIGNED_DECIMAL.equalsIgnoreCase(fieldModel.getDataType())
										|| UobConstants.SRC_NUMERIC.equalsIgnoreCase(fieldModel.getDataType())) {
									if (columnValuesToIgnore.contains(value.toLowerCase())) {
										value = "";
									}
									if (StringUtils.trimToNull(value) != null) {
										try {
											value = value.replaceAll(UobConstants.COMMA, UobConstants.EMPTY);
											new BigDecimal(value);
										} catch (NumberFormatException nfe) {

										}
									}

								} else if (UobConstants.SRC_TIMESTAMP.equalsIgnoreCase(fieldModel.getDataType())) {
									SimpleDateFormat sdf = new SimpleDateFormat(
											DateConverter.getEquivalentJavaFormat(fieldModel.getDataFormat()));
									try {
										value = sdf.format(new Date(Long.valueOf(value)));
									} catch (NumberFormatException nfe) {
										try {
											sdf.parse(value);
										} catch (ParseException e) {
											value = UobConstants.EMPTY;
										}
									}
								}
								String charsetString = new String(
										new String(value.getBytes(), Charset.forName("UTF-8")).getBytes(characterSet));
								sb.append(charsetString).append(delimiter);
							}
							noOfRecords++;
							writer.write(sb.substring(0, (sb.length() - delimiter.length())));
							writer.newLine();
						}
					}
				}
				if (isHeaderAvailable && !foundHeader && noOfRecords == 0) {
					fieldNames.removeAll(this.fieldNames);
					throw new EDAGProcessorException(EDAGProcessorException.MISSING_HEADER_ROW_DEFINITION, sourcefile,
							StringUtils.join(fieldNames.toArray(new String[0]), UobConstants.COMMA));
				}
			} else {
				throw new EDAGProcessorException(EDAGProcessorException.MISSING_SHEET_DEFINITION, sourcefile);
			}
		} catch (EDAGProcessorException e) {
			File file = new File(fileModel.getSourceDirectory());
			if (file.exists()) {
				file.delete();
			}
			throw e;
		} catch (Exception e) {
			File file = new File(fileModel.getSourceDirectory());
			if (file.exists()) {
				file.delete();
			}
			throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, e);
		}
		return noOfRecords;
	}

	private boolean isHeaderRow(Row row, Map<String, List<String>> fieldNamePatterns, FileModel fileModel, String regexExpression)
			throws EDAGPOIException, EDAGValidationException {

		logger.debug("Reading Header Row");

		Pattern regexPattern = Pattern.compile("[a-zA-Z0-9-!$#@%^&*()_+|~=`{}\\[\\]:\";'<>?,.\\/]*");
		int expectedNoOfColumns = fileModel.getSrcFieldInfo().size();
		if (row == null) {
			return Boolean.FALSE;
		}

		int loopExitColSize = row.getLastCellNum() > expectedNoOfColumns ? expectedNoOfColumns : row.getLastCellNum();
		List<String> cellValues = new ArrayList<>(expectedNoOfColumns);
		for (int cellNum = 0; cellNum < loopExitColSize; cellNum++) {
			String cellValue = POIUtils.getCellContent(row, cellNum, String.class);
			cellValue = cellValue.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase();
			Matcher matcher = regexPattern.matcher(cellValue);
			if (!matcher.matches()) {
				return Boolean.FALSE;
			}
			if (StringUtils.trimToNull(cellValue) != null) {
				cellValues.add(cellValue);
			}
		}

		int headerFieldCount = 0;
		List<String> matchedFieldNames = new ArrayList<>();
		List<String> fieldNames = fileModel.getSrcFieldInfo().stream().map(f -> f.getFieldName()).collect(Collectors.toList());
		for (int i = 0; i < fieldNames.size(); i++) {
			String fieldName = fieldNames.get(i);
			List<String> fieldMappingEntries = fieldNamePatterns.get(fieldName);
			if (fieldMappingEntries != null) {
				Optional<String> fieldToHeaderFieldMapping = fieldMappingEntries.stream()
						.map(action -> action.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase())
						.filter(action -> cellValues.contains(action)).findFirst();
				if (fieldToHeaderFieldMapping.isPresent()) {
					headerFieldCount++;
					matchedFieldNames.add(fieldName);
				}
			}
		}

		this.headerFieldCount = this.headerFieldCount < headerFieldCount ? headerFieldCount : this.headerFieldCount;
		if (!matchedFieldNames.isEmpty() && matchedFieldNames.size() > this.fieldNames.size()) {
			this.fieldNames = matchedFieldNames;
		}

		return cellValues.size() == expectedNoOfColumns && matchedFieldNames.size() == expectedNoOfColumns;
	}
	
	private boolean isHeaderRowForTranspose(Row row, Map<String, List<String>> fieldNamePatterns, FileModel fileModel, String regexExpression)
			throws EDAGPOIException, EDAGValidationException {

		logger.debug("Reading Header Row");

		Pattern regexPattern = Pattern.compile("[a-zA-Z0-9-!$#@%^&*()_+|~=`{}\\[\\]:\";'<>?,.\\/]*");
		int expectedNoOfColumns = fileModel.getSrcFieldInfo().size();
		if (row == null) {
			return Boolean.FALSE;
		}

		int loopExitColSize = row.getLastCellNum(); //> expectedNoOfColumns ? expectedNoOfColumns : row.getLastCellNum();
		List<String> cellValues = new ArrayList<>(row.getLastCellNum());
		for (int cellNum = 0; cellNum < loopExitColSize; cellNum++) {
			String cellValue = POIUtils.getCellContent(row, cellNum, String.class);
			cellValue = cellValue.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase();
			Matcher matcher = regexPattern.matcher(cellValue);
			if (!matcher.matches()) {
				return Boolean.FALSE;
			}
			if (StringUtils.trimToNull(cellValue) != null) {
				cellValues.add(cellValue);
			}
		}

		int headerFieldCount = 0;
		List<String> matchedFieldNames = new ArrayList<>();
		List<String> fieldNames = fileModel.getSrcFieldInfo().stream().map(f -> f.getFieldName()).collect(Collectors.toList());
		for (int i = 0; i < fieldNames.size(); i++) {
			String fieldName = fieldNames.get(i);
			List<String> fieldMappingEntries = fieldNamePatterns.get(fieldName);
			if(fieldMappingEntries.get(0).contains("{$year}")) {
				for(int j=0;j<this.noOfYears;j++)
				{
					List<String> tempStr = new ArrayList<String>();
					tempStr.add(fieldMappingEntries.get(0).replace("{$year}", String.valueOf(startYearCount+j)));
					Optional<String> fieldToHeaderFieldMapping = tempStr.stream()
							.map(action -> action.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase())
							.filter(action -> cellValues.contains(action)).findFirst();
					if (fieldToHeaderFieldMapping.isPresent()) {
						headerFieldCount++;
						matchedFieldNames.add(fieldName);
					}
				}
				
			}
			else if (fieldMappingEntries != null) {
				Optional<String> fieldToHeaderFieldMapping = fieldMappingEntries.stream()
						.map(action -> action.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase())
						.filter(action -> cellValues.contains(action)).findFirst();
				if (fieldToHeaderFieldMapping.isPresent() || fieldMappingEntries.get(0).equalsIgnoreCase("YEAR")) {
					headerFieldCount++;
					matchedFieldNames.add(fieldName);
				}
			}
		}

		this.headerFieldCount = this.headerFieldCount < headerFieldCount ? headerFieldCount : this.headerFieldCount;
		if (!matchedFieldNames.isEmpty() && matchedFieldNames.size() > this.fieldNames.size()) {
			this.fieldNames = matchedFieldNames;
		}

		return cellValues.size() == row.getLastCellNum() && matchedFieldNames.size() == row.getLastCellNum()+1;
	}

	private boolean isHeaderAvailable(FileLayoutIdentifier fileLayout) {
		return fileLayout == FileLayoutIdentifier.XLS_WITH_HEADER || fileLayout == FileLayoutIdentifier.XLSX_WITH_HEADER;
	}

	private Workbook createWorkBookBasedOfFileType(FileLayoutIdentifier fileTypeIdentifier, String completeFilePath) throws Exception {
		Workbook workbook = null;
		switch (fileTypeIdentifier) {
		case XLS_WITH_HEADER:
		case XLS_WITHOUT_HEADER:
			workbook = new HSSFWorkbook(new FileInputStream(new File(completeFilePath)));
			break;
		case XLSX_WITH_HEADER:
		case XLSX_WITHOUT_HEADER:
//			workbook = new XSSFWorkbook(new FileInputStream(new File(completeFilePath)));
//			logger.info("Streaming Excel source files: completeFilePath: "+completeFilePath);
//			File file = new File(completeFilePath);
			
			String enabledStreamFlag = PropertyLoader.getProperty(UobConstants.EXCEL_STREAM_ENABLE_FLAG);
			
			if (enabledStreamFlag == null || !enabledStreamFlag.equalsIgnoreCase("TRUE")) {
				logger.info("Excel file ingestion is in the normal mode.");
				workbook = new XSSFWorkbook(new FileInputStream(new File(completeFilePath)));
				
			}else {
				logger.info("Excel file ingestion is in the stream mode.");
				int rowCacheSize = Integer.valueOf(PropertyLoader.getProperty(UobConstants.EXCEL_STREAM_ROW_CATCH_SIZE));
				int bufferSize = Integer.valueOf(PropertyLoader.getProperty(UobConstants.EXCEL_STREAM_BUFFER_SIZE));
				
				InputStream is = new FileInputStream(new File(completeFilePath));
				workbook = StreamingReader.builder().rowCacheSize(rowCacheSize).bufferSize(bufferSize).open(is);
				
			}
			
			break;
		default:
			throw new EDAGPOIException("Invalid file type format");
		}
		return workbook;
	}

	private List<Integer> readHeaderRow(Row row, Map<String, List<String>> fieldNamePatterns, FileModel fileModel,
			String regexExpression) throws EDAGPOIException, EDAGValidationException, EDAGProcessorException {

		List<FieldModel> fieldModelList = fileModel.getSrcFieldInfo();
		int expectedNoOfColumns = fieldModelList.size();
		Integer[] cellNumberOrder = new Integer[expectedNoOfColumns];
		Map<String, Integer> fieldNameToFieldNUmberMapping = new HashMap<>();

		for (FieldModel fieldModel : fieldModelList) {
			fieldNameToFieldNUmberMapping.put(fieldModel.getFieldName().toLowerCase(), fieldModel.getFieldNum());
		}

		List<String> fieldNames = fileModel.getSrcFieldInfo().stream().map(f -> f.getFieldName()).collect(Collectors.toList());

		int loopExitColSize = row.getLastCellNum() > expectedNoOfColumns ? expectedNoOfColumns : row.getLastCellNum();
		Map<String, Integer> cellValuesMap = new HashMap<>(expectedNoOfColumns);

		for (int cellNum = 0; cellNum < loopExitColSize; cellNum++) {
			String cellValue = POIUtils.getCellContent(row, cellNum, String.class);
			cellValuesMap.put(cellValue.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase(), cellNum);
		}

		for (int i = 0; i < fieldNames.size(); i++) {
			Integer cellNum = null;
			String fieldName = fieldNames.get(i);
			List<String> fieldMappingEntries = fieldNamePatterns.get(fieldName);
			if (fieldMappingEntries != null) {
				Optional<String> available = fieldMappingEntries.stream()
						.filter(action -> cellValuesMap.keySet()
								.contains(action.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase()))
						.findFirst();
				if (available.isPresent()) {
					cellNum = cellValuesMap
							.get(available.get().replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase());
					cellNumberOrder[i] = cellNum;
				}
			}

			if (cellNum == null) {
				throw new EDAGProcessorException(EDAGProcessorException.MISSING_HEADER_FIELD_DEFINITION, fieldName);
			} else {
				cellNumberOrder[i] = cellNum;
			}
		}

		for (int i = 0; i < cellNumberOrder.length; i++) {
			for (int j = i + 1; j < cellNumberOrder.length; j++) {
				if (i != j && cellNumberOrder[i] == cellNumberOrder[j]) {
					String headerColumn = POIUtils.getCellContent(row, cellNumberOrder[i], String.class);
					throw new EDAGProcessorException(EDAGProcessorException.DUPLICATE_EXCEL_HEADER_FIELD_MAPPING, fieldNames.get(i),
							fieldNames.get(j), headerColumn);
				}
			}
		}
		return Arrays.asList(cellNumberOrder);
	}
	
	private List<String> readHeaderRowForTranspose(Row row, Map<String, List<String>> fieldNamePatterns, FileModel fileModel,
			String regexExpression) throws EDAGPOIException, EDAGValidationException, EDAGProcessorException {

		List<FieldModel> fieldModelList = fileModel.getSrcFieldInfo();
		int expectedNoOfColumns = fieldModelList.size();
		String[] cellNumberOrder = new String[expectedNoOfColumns];
		Map<String, Integer> fieldNameToFieldNUmberMapping = new HashMap<>();

		for (FieldModel fieldModel : fieldModelList) {
			fieldNameToFieldNUmberMapping.put(fieldModel.getFieldName().toLowerCase(), fieldModel.getFieldNum());
		}

		List<String> fieldNames = fileModel.getSrcFieldInfo().stream().map(f -> f.getFieldName()).collect(Collectors.toList());

		int loopExitColSize = row.getLastCellNum();// > expectedNoOfColumns ? expectedNoOfColumns : row.getLastCellNum();
		Map<String, Integer> cellValuesMap = new HashMap<>(row.getLastCellNum());

		for (int cellNum = 0; cellNum < loopExitColSize; cellNum++) {
			String cellValue = POIUtils.getCellContent(row, cellNum, String.class);
			cellValuesMap.put(cellValue.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase(), cellNum);
		}

		for (int i = 0; i < fieldNames.size(); i++) {
			Integer cellNum = null;
			String fieldName = fieldNames.get(i);
			List<String> fieldMappingEntries = fieldNamePatterns.get(fieldName);
			if (fieldMappingEntries != null) {
				if(fieldMappingEntries.get(0).contains("{$year}"))
				{
					cellNumberOrder[i] = "";
					for(int j=0;j<noOfYears;j++) {
						List<String> tempStr = new ArrayList<String>();
						tempStr.add(fieldMappingEntries.get(0).replace("{$year}", String.valueOf((startYearCount+j))));
						Optional<String> available = tempStr.stream()
								.filter(action -> cellValuesMap.keySet()
								.contains(action.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase()))
								.findFirst();
						if (available.isPresent()) {
							if(j>0)
								cellNumberOrder[i] = cellNumberOrder[i] + "#";
							cellNum = cellValuesMap
									.get(available.get().replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase());
							cellNumberOrder[i] = cellNumberOrder[i] + cellNum;
						}
					}
					if (cellNum == null) {
						throw new EDAGProcessorException(EDAGProcessorException.MISSING_HEADER_FIELD_DEFINITION, fieldName);
					} /*else {
						cellNumberOrder[i] = String.valueOf(cellNum);
					}*/
				}
				else {
				Optional<String> available = fieldMappingEntries.stream()
						.filter(action -> cellValuesMap.keySet()
								.contains(action.replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase()))
						.findFirst();
				if(fieldMappingEntries.get(0).equalsIgnoreCase("YEAR"))
				{
					cellNumberOrder[i] = "{$year}";
					cellNum=0;
				}
				else if (available.isPresent()) {
					cellNum = cellValuesMap
							.get(available.get().replaceAll(REGULAR_EXPRESSION, "").replaceAll(regexExpression, "").toLowerCase());
					cellNumberOrder[i] = String.valueOf(cellNum);
				}

				if (cellNum == null) {
					throw new EDAGProcessorException(EDAGProcessorException.MISSING_HEADER_FIELD_DEFINITION, fieldName);
				} else {
					if(!fieldMappingEntries.get(0).equalsIgnoreCase("YEAR"))
						cellNumberOrder[i] = String.valueOf(cellNum);
				}
				}
		}
		}

		/*for (int i = 0; i < cellNumberOrder.length; i++) {
			for (int j = i + 1; j < cellNumberOrder.length; j++) {
				if (i != j && cellNumberOrder[i] == cellNumberOrder[j]) {
					String headerColumn = POIUtils.getCellContent(row, cellNumberOrder[i], String.class);
					throw new EDAGProcessorException(EDAGProcessorException.DUPLICATE_EXCEL_HEADER_FIELD_MAPPING, fieldNames.get(i),
							fieldNames.get(j), headerColumn);
				}
			}
		}*/
		return Arrays.asList(cellNumberOrder);
	}

	private boolean checkIfRowIsEmpty(Row row, int expectedNoOfColumns) {
		Boolean[] columnns = new Boolean[expectedNoOfColumns];
		if (row == null || row.getLastCellNum() <= 0) {
			return Boolean.TRUE;
		}

		for (int cellNum = row.getFirstCellNum(); cellNum < expectedNoOfColumns; cellNum++) {
			Cell cell = row.getCell(cellNum);
			if (cell != null && cell.getCellTypeEnum() == CellType.BLANK) {
				columnns[cellNum] = Boolean.TRUE;
			} else {
				columnns[cellNum] = Boolean.FALSE;
			}
		}
		return Arrays.asList(columnns).stream().filter(f -> f == Boolean.TRUE).count() == expectedNoOfColumns;
	}
	
	private boolean checkRowsBeforeHeader(Row row, int expectedNoOfColumns) {
		Boolean[] columnns = new Boolean[expectedNoOfColumns];
		if (row == null || row.getLastCellNum() <= 0) {
			return Boolean.TRUE;
		}

		for (int cellNum = row.getFirstCellNum(); cellNum < expectedNoOfColumns; cellNum++) {
			Cell cell = row.getCell(cellNum);
			if (cell != null && cell.getCellTypeEnum() == CellType.BLANK) {
				columnns[cellNum] = Boolean.TRUE;
			} else {
				columnns[cellNum] = Boolean.FALSE;
			}
		}
		return Arrays.asList(columnns).stream().filter(f -> f == Boolean.TRUE).count() >= 1;
	}

	public static void main(String[] args) throws EDAGMyBatisException, EDAGValidationException {
		ExcelFileIngestionProcessor fileIngestor = new ExcelFileIngestionProcessor();
		ProcessModel procModel = fileIngestor.retrieveMetadata("FI_CED_ACRA1_Q01", "SG");
		procModel.getSrcInfo().setSourceArchivalDir("/Users/bj186016/Code/ThinkBig/UOB/ACRA1");
		procModel.getSrcInfo().setSourceDirectory("/Users/bj186016/Code/ThinkBig/UOB/testFiles/testfiles/SG_ACRA/ACRA1");

		ProcessInstanceModel procInstanceModel = new ProcessInstanceModel(procModel);
		procInstanceModel.setBizDate("2018-09-05");
		procInstanceModel.setCountryCd("CN");
		procInstanceModel.setProcInstanceId("FI_CED_ACRA2_Q01");

		try {
			fileIngestor.runFileIngestion(procInstanceModel, procModel, "2018-09-05", "CN", true, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
