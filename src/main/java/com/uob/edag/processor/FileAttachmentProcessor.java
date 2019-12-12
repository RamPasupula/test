package com.uob.edag.processor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import com.uob.edag.model.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;

import com.teradata.hook.tdinterfaces.HookFrameworkInterface;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.dao.ImpalaDao;
import com.uob.edag.dao.IngestionDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.io.EDAGFileReader;
import com.uob.edag.model.StageModel.Status;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.HadoopUtils;
import com.uob.edag.utils.MemoryLogger;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;
import com.uob.edag.utils.VelocityUtils;

public class FileAttachmentProcessor extends FileIngestionProcessor implements IngestionProcessor {

	private long noOfRecords = 0;
	private Map<FieldModel, List<String>> fieldModelFileReferenceMap = new HashMap<>();
	private Map<FieldModel, Integer> fileReferenceCountMap = new HashMap<>();
	private List<FieldModel> fileReferenceFieldsList;
	private static Options options = new Options();	
	private static Logger staticLogger;
	private boolean seperateIndexingJob;

	@Override
	public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel processModel, String bizDate, String ctryCd,
			boolean forceRerun, String forceFileName) throws EDAGException {

		logger.info("Going to run Ingestion for file containing attachments for process: " + processModel.getProcId());
		setAttachmentInformation(processModel, ctryCd, bizDate);
		FileModel fileModel = processModel.getSrcInfo();

		if (!HadoopUtils.checkIfFileExists(fileModel.getHdfsTargetFolder())) {
			HadoopUtils.createHDFSPath(fileModel.getHdfsTargetFolder());
		}

		String sourceFile = fileModel.getSourceDirectory();
		File inputFile = new File(sourceFile);
		if (!inputFile.exists()) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, inputFile.getPath(), "File doesn't exist");
		}
		String charsetName = PropertyLoader.getCharsetName(procInstanceModel.getCountryCd());
		Charset charset = Charset.forName(charsetName);

		FileUtility fileUtils = FileUtilityFactory.getFileUtility(processModel);
		String footer = fileUtils.readLineByIndicator(sourceFile, UobConstants.LAST, charsetName);
		String delim = StringUtils.trimToEmpty(fileModel.getColumnDelimiter());

		fileReferenceFieldsList = fileModel.getSrcFieldInfo().stream()
				.filter(f -> f.getDataType().equalsIgnoreCase(UobConstants.SRC_FILE_REFERENCE))
				.sorted((f1, f2) -> Integer.compare(f1.getFieldNum(), f2.getFieldNum())).collect(Collectors.toList());


		try (EDAGFileReader fileReader = new EDAGFileReader(new InputStreamReader(new FileInputStream(inputFile), charset), footer)) {

			fileReferenceFieldsList.stream().forEach(action -> {
				fileReferenceCountMap.put(action, 0);
				fieldModelFileReferenceMap.put(action, new ArrayList<>());
			});

			readFileReferences(fileReader, processModel); // modified, sm186140

			Map<FieldModel, Integer> copiedFileReferenceCountMap = copyReferenceFilesToHDFSPath(processModel.getProcId(), ctryCd,
					bizDate, fileModel, fieldModelFileReferenceMap);
			writeToStageLog(procInstanceModel, processModel, fileReferenceCountMap, copiedFileReferenceCountMap, noOfRecords);
			archiveReferenceFiles(processModel.getProcId(), ctryCd, bizDate, fileModel, fieldModelFileReferenceMap);
			reconcileReferenceFileCountCheck(processModel.getProcId(), ctryCd, bizDate, noOfRecords, fileReferenceFieldsList,
					fileReferenceCountMap, copiedFileReferenceCountMap);

			super.runFileIngestion(procInstanceModel, processModel, bizDate, ctryCd, forceRerun, forceFileName);
			if(!procInstanceModel.isSkipIndexingEnabled())
			{
				setSeperateIndexingJobFlag(Boolean.FALSE);
				checkAndCallIndexingProcess(procInstanceModel, processModel, bizDate, ctryCd);
			}
			
		} catch (FileNotFoundException fe) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, sourceFile, fe.getMessage()); 
		} catch (IOException ie) {
			throw new EDAGIOException("Unable to read from " + inputFile.getPath() + ". Exception occurred: " + ie.getMessage());
		} catch (Exception e) {
			throw new EDAGIOException(e.getMessage());
		}
	}


	// created,  sm186140
	private void buildFileReferenceList(String line, FileModel fileModel){

		this.noOfRecords++; // keep track of number records
		// redundant, keep for compatibility/refactoring purpose
		String delim = StringUtils.trimToEmpty(fileModel.getColumnDelimiter());

		String[] fieldValues = extractFieldsFromLine(line, fileModel.getSourceFileLayoutCd(), delim, fileReferenceFieldsList);
		for (int i = 0; i < this.fileReferenceFieldsList.size(); i++) {
			FieldModel fieldModel = this.fileReferenceFieldsList.get(i);
			String fileReferenceFileName = fieldValues[i];
			if (StringUtils.trimToNull(fileReferenceFileName) != null) {
				if (this.fieldModelFileReferenceMap.get(fieldModel) == null) {
					List<String> fileReferenceList = new ArrayList<>();
					this.fieldModelFileReferenceMap.put(fieldModel, fileReferenceList);
				}
				this.fieldModelFileReferenceMap.get(fieldModel).add(StringUtils.trim(fileReferenceFileName));
				this.fileReferenceCountMap.put(fieldModel, this.fileReferenceCountMap.get(fieldModel) + 1);
			}
		}
	}

	// created,  sm186140
	private void readFileReferences(EDAGFileReader reader, ProcessModel procModel) throws EDAGValidationException, IOException {

		FileModel sourceInfo = procModel.getSrcInfo();
		HadoopModel destInfo = procModel.getDestInfo();

		SortedMap<Integer, FieldModel> fieldMap = new TreeMap<>(destInfo.getDestFieldInfo());
		String delim = StringUtils.trimToEmpty(sourceInfo.getColumnDelimiter());
		String quote = StringUtils.trimToEmpty(sourceInfo.getTextDelimiter());
		String line = null;
		String fileLayout = sourceInfo.getSourceFileLayoutCd();
		String unescapedDelim = StringEscapeUtils.unescapeJava(delim);
		String targetLine = null;

		logger.info("Starting new line management for file attachment processor and reference file reads..");

		while ((line = reader.readLine()) != null) {

			// Do new Line management
			if (UobConstants.FIXED_FILE.equalsIgnoreCase(fileLayout)){

				int lastFieldEndPosition = fieldMap.get(fieldMap.lastKey()).getEndPosition();
				String nextLine = null;

				while (line.length() < lastFieldEndPosition && ((nextLine = reader.readLine()) != null)) {
					logger.debug("File Attachment Processor: Line[" + reader.getLineNumber() + "] length (" + line.length()
							+ ") is less than field end position (" + lastFieldEndPosition + "), adding next line ("
							+ nextLine.length() + " chars) to line");
					line += " " + nextLine;

				}

				if (line.length() != lastFieldEndPosition) {
					// we reach EOF and line length is still not the same as last field's end position
					throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH,
							line.length(), lastFieldEndPosition, "Line number: " + reader.getLineNumber() + ", line: " + line);
				}

				buildFileReferenceList(line, sourceInfo);
				// do something

			}
			else if (UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(sourceInfo.getSourceFileLayoutCd())){

				targetLine = "";
				boolean newLine = true;
				int lastFieldEndPosition = -1;
				for (FieldModel field : fieldMap.values()) {
					String nextLine = null;
					while (line.length() < field.getEndPosition() && ((nextLine = reader.readLine()) != null)) {
						logger.debug("File Attachment Processor: Line[" + reader.getLineNumber() + "] length (" + line.length()
								+ ") is less than field end position (" + field.getEndPosition()
								+ "), adding next line (" + nextLine.length() + " chars) to line");
						line += " " + nextLine;
					}

					if (line.length() < field.getEndPosition()) {
						// we reach EOF and line is still unable to cater for current field
						throw new EDAGValidationException(EDAGValidationException.LINE_TOO_SHORT,
								line.length(), field.getFieldName(), field.getStartPosition(),
								field.getEndPosition(), "Line: " + line);
					}


					String value = quote + StringUtils.trimToEmpty(line.substring(field.getStartPosition(),
							field.getEndPosition())) + quote;

					targetLine += newLine ? value : unescapedDelim + value;
					newLine = false;
					lastFieldEndPosition = field.getEndPosition();
				}

				if (lastFieldEndPosition < line.length()) {
					throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH, line.length(),
							lastFieldEndPosition, "Line no: " + reader.getLineNumber() + ", line: " + line);
				}

				buildFileReferenceList(targetLine, sourceInfo);

			}
			else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(fileLayout)) {
				String escapedDelim = delim.replaceAll("\\|", "\\\\\\|");
				String nextLine = null;
				String[] fields = line.split(escapedDelim, -1);

				while (fields.length < fieldMap.size() && ((nextLine = reader.readLine()) != null)) {

					logger.debug("File Attachment Processor: Parsed field count (" + fields.length + ") is less than expected field count("
							+ fieldMap.size() + "), adding next line (" + nextLine.length() + " chars) to line["
							+ reader.getLineNumber() + "]");
					line = line + " " + nextLine;
					fields = line.split(escapedDelim, -1);
					logger.debug("New field count is " + fields.length);
				}

				if (fields.length != fieldMap.size()) {
					// we reach EOF and number of fields parsed from current line is still less than expected number of fields
					throw new EDAGValidationException(EDAGValidationException.FIELD_COUNT_MISMATCH,
							fields.length, fieldMap.size(), "Line no: " + reader.getLineNumber() + ", line: " + line);
				}

				buildFileReferenceList(line, sourceInfo);

			}

		}

		logger.info("End of new line management for file attachment processor.");
		logger.info("Total records read:" + noOfRecords);
	}
	
	private HashMap<String, HashMap<String, String>> prepareParamsForIndexing(ProcessInstanceModel procInstanceModel, ProcessModel processModel, String bizDate, String ctryCd) throws EDAGMyBatisException, EDAGIOException {
		
		//		1.hdfsInputPath
		//		3.InputList - list of file names and collection names   map
		//		4.regexLists - List of collection names and regex       map
		//		5.procInstanceId
		//		6.HiveRowCount  
		
		HiveDao hiveDao = new HiveDao();
		ImpalaDao impalaDao = new ImpalaDao();
		IngestionDao ingestDao = new IngestionDao();
		
		HashMap<String, HashMap<String, String>> sparkInputListMap = new HashMap<String, HashMap<String, String>>();
		HashMap<String, String> collectionRegxMap = new HashMap<String, String>();
		List<SolrCollectionModel> solrCollectionModelList = ingestDao.retrieveSolrCollection();
		
		HashMap<String, String> hdfsInputPathMap = new HashMap<String, String>();
		
		// TODO: remove hard coded source codes.
		hdfsInputPathMap.put(UobConstants.UNS_HDFS_INPUT_PATH, processModel.getSrcInfo().getHdfsTargetFolder());
		logger.info("** processModel.getSrcInfo().getHdfsTargetFolder() : " + processModel.getSrcInfo().getHdfsTargetFolder());		
		
//		hdfsInputPathMap.put("UNS_HDFS_INPUT_PATH", PropertyLoader.getProperty("UNS_HDFS_INPUT_PATH"));
		
		// 3.InputList - list of file names and collection names   combine key - value (files names - collection name)
		HashMap<String, String> fileCollectionMap = new HashMap<String, String>();
		List<UNSFileTypeModel> UNSFileTypeModelList = new ArrayList<UNSFileTypeModel>();
		
		// ORACLE query
		String hiveSQL = ingestDao.retrieveT11HiveQL(processModel.getProcId()).get(0).getHiveQL();
		String engineName = ingestDao.retrieveT11HiveQL(processModel.getProcId()).get(0).getEngineName();
		
		// start: added by Tyler on 24th May for supporting where condition replacement.
		if (hiveSQL.contains(UobConstants.BIZ_DATE_STRING)) {
			hiveSQL = hiveSQL.replaceAll(UobConstants.BIZ_DATE_STRING, "'" + bizDate + "'");
		}
		if (hiveSQL.contains(UobConstants.COUNTRY_CODE_STRING)) {
			hiveSQL = hiveSQL.replaceAll(UobConstants.COUNTRY_CODE_STRING, "'" + ctryCd + "'");
		}
		// end: added by Tyler on 24th May for supporting where condition replacement.
		
		logger.info("**** engineName: " + engineName);
		logger.info("**** hiveSQL: " + hiveSQL);
		
		// HIVE query
		if (engineName.equalsIgnoreCase(UobConstants.IMPALA)) {
			UNSFileTypeModelList = impalaDao.getAttachNamesWithDocTypeImp(hiveSQL).stream()
					.filter(f -> UobConstants.UNS_FILE_TYPES.contains(f.getFileType())).collect(Collectors.toList());
		} else {
			UNSFileTypeModelList = hiveDao.getAttachmentNamesWithDocType(hiveSQL).stream()
					.filter(f -> UobConstants.UNS_FILE_TYPES.contains(f.getFileType())).collect(Collectors.toList());
		}
		
		logger.info("**** UNSFileTypeModelList.size(): " + UNSFileTypeModelList.size());
		logger.info("**** solrCollectionModelList.size(): " + solrCollectionModelList.size());


		UNSFileTypeModelList.stream().forEach(unsFileTypemodel -> {
			solrCollectionModelList.stream().forEach(solrCollectModel -> {

				String hivefileDocTypewithName = unsFileTypemodel.getCountryCode() + unsFileTypemodel.getFileType()+ unsFileTypemodel.getDocumentTypeCode();
				String soleMetafileDoctypewithName = solrCollectModel.getCountryCode() + solrCollectModel.getFileType() + solrCollectModel.getDocumentTypeCode();

				hivefileDocTypewithName = hivefileDocTypewithName.replace(" ", "");
				soleMetafileDoctypewithName = soleMetafileDoctypewithName.replace(" ", "");

				if (hivefileDocTypewithName.equalsIgnoreCase(soleMetafileDoctypewithName)
						&& !fileCollectionMap.containsKey(unsFileTypemodel.getFileName().substring(unsFileTypemodel.getFileName().lastIndexOf("/")+1))) {

					//logger.info("**** hivefileDocTypewithName: " + hivefileDocTypewithName);

					// file name - collection mapping
					fileCollectionMap.put(unsFileTypemodel.getFileName().substring(unsFileTypemodel.getFileName().lastIndexOf("/")+1), solrCollectModel.getCollectionName());

					// collection-regx mapping
					if (!collectionRegxMap.containsKey(solrCollectModel.getCollectionName())) {
						collectionRegxMap.put(solrCollectModel.getCollectionName(), solrCollectModel.getRegularExp());
					}

//					solrCollectionModelList.remove(solrCollectModel);
				}
			});
		});

		logger.info("**** fileCollectionMap size : " + fileCollectionMap.size());
		logger.info("**** collectionRegxMap size : " + collectionRegxMap.size());
		
		//procInstanceId
		HashMap<String, String> procInstanceIdMap = new HashMap<String, String>();
		procInstanceIdMap.put(UobConstants.PROC_INSTANCE_ID, procInstanceModel.getProcInstanceId());
		procInstanceIdMap.put(UobConstants.SRC_SYS_CD, processModel.getSrcSysCd());
		
		//HiveRowCount
		HashMap<String, String> hdfsHostMap = new HashMap<String, String>();
		hdfsHostMap.put(UobConstants.HDFS_HOST_NAME, HadoopUtils.getHDFSHostName());
		
		//previous run process instance id
		boolean fetchPrevProcInstanceId = !this.getSeperateIndexingJobFlag();
		logger.info("Fetching PrevProcInstanceId Flag.." + fetchPrevProcInstanceId);
		String prevRunProcInstanceId = null;
		if(fetchPrevProcInstanceId)
			prevRunProcInstanceId = ingestDao.retrievePrevRunProcInstanceId(procInstanceModel);
		else
			prevRunProcInstanceId = procInstanceModel.getProcInstanceId();
		HashMap<String, String> prevProcInstanceIdMap = new HashMap<String, String>();
		prevProcInstanceIdMap.put("prevProcInstanceId", prevRunProcInstanceId);
		logger.info("**** prevProcInstanceId : " + prevRunProcInstanceId);
		
		sparkInputListMap.put(UobConstants.HDFSINPUTPATH, hdfsInputPathMap);
		sparkInputListMap.put(UobConstants.INPUTLIST, fileCollectionMap);
		sparkInputListMap.put(UobConstants.REGEXLISTS, collectionRegxMap);
		sparkInputListMap.put(UobConstants.PROC_INSTANCE_ID, procInstanceIdMap);
		sparkInputListMap.put(UobConstants.HDFS_HOST_NAME, hdfsHostMap);
		sparkInputListMap.put("prevProcInstanceId", prevProcInstanceIdMap);
		
		/*sparkInputListMap.entrySet().stream().forEach(mapElement -> {
			mapElement.getValue().entrySet().stream().forEach(subMap -> {
				logger.info("File Attachment sub map key-value pairs: " + subMap.getKey() + " || " + subMap.getValue());
			});
		});*/
		
		logger.info("** prepareParamsForIndexing is done **");

		return sparkInputListMap;
	}

	private void setAttachmentInformation(ProcessModel processModel, String ctryCd, String bizDate) {
		logger.info("Retrieving file attachment information for process ID: " + processModel.getProcId());
		Context ctx = new VelocityContext();
		ctx.put(UobConstants.SOURCESYSTEMCODE, processModel.getSrcSysCd());
		ctx.put(UobConstants.PROCESSID, processModel.getProcId());
		ctx.put(UobConstants.COUNTRYCODE, ctryCd);
		ctx.put(UobConstants.BUSINESSDATE, bizDate);
		ctx.put(UobConstants.SOURCEFILENAME, processModel.getSrcInfo().getSourceFileName());

		CountryAttributes countryAttribute = processModel.getCountryAttributesMap().get(ctryCd);
		String referenceFolder = countryAttribute.getReferencedFileFolder();
		if (StringUtils.trimToNull(referenceFolder) == null) {
			referenceFolder = ProcessModel.DEFAULT_REFERENCED_FILE_SOURCE_FOLDER;
		}

		for (ProcessParam procParam : processModel.getProcParam()) {
			if (!procParam.getParamName().equals(UobConstants.PROC_PARAM_REFERENCED_SOURCE_FOLDER)
					&& procParam.getParamName().startsWith(UobConstants.PROC_PARAM_REFERENCED_SOURCE_FOLDER)) {
				ctx.put(procParam.getParamName().substring(UobConstants.PROC_PARAM_REFERENCED_SOURCE_FOLDER.length() + 1),
						procParam.getParamValue());
			}
		}

		StringWriter out = new StringWriter();
		VelocityUtils.evaluate(ctx, out, "Evaluating value for " + referenceFolder, referenceFolder);
		processModel.getSrcInfo().setReferenceFolderName(out.toString());

		String hdfsTargetFolder = UobConstants.EMPTY;
		for (ProcessParam procParam : processModel.getProcParam()) {
			if (procParam.getParamName().equals(UobConstants.PROC_PARAM_REFERENCED_TARGET_FOLDER)) {
				hdfsTargetFolder = procParam.getParamValue();
			} else if (procParam.getParamName().startsWith(UobConstants.PROC_PARAM_REFERENCED_TARGET_FOLDER)) {
				ctx.put(procParam.getParamName().substring(UobConstants.PROC_PARAM_REFERENCED_TARGET_FOLDER.length() + 1),
						procParam.getParamValue());
			}
		}

		if (StringUtils.trimToNull(hdfsTargetFolder) == null) {
			hdfsTargetFolder = ProcessModel.DEFAULT_HDFS_TARGE_FOLDER;
		}

		out = new StringWriter();
		VelocityUtils.evaluate(ctx, out, "Evaluating value for " + hdfsTargetFolder, hdfsTargetFolder);
		processModel.getSrcInfo().setHdfsTargetFolder(out.toString());

		String hdfsSuffixValue = UobConstants.EMPTY;
		for (ProcessParam procParam : processModel.getProcParam()) {
			if (procParam.getParamName().equals(UobConstants.PROC_PARAM_REFERENCED_FILE_FIELDNAME_SUFFIX)) {
				hdfsSuffixValue = procParam.getParamValue();
			}
		}
		if (StringUtils.trimToNull(hdfsSuffixValue) == null) {
			hdfsSuffixValue = ProcessModel.DEFAULT_HDFS_PATH_SUFFIX;
		}
		processModel.getSrcInfo().setHdfsPathSuffix(hdfsSuffixValue);

		logger.info(String.format("Reference file folder location is %s", processModel.getSrcInfo().getReferenceFolderName()));
		logger.info(String.format("Target folder location is %s", processModel.getSrcInfo().getHdfsTargetFolder()));
	}

	private String[] extractFieldsFromLine(String fileLine, String fileLayout, String delim, List<FieldModel> referenceFileFieldList) {
		String[] fieldValues = new String[referenceFileFieldList.size()];

		if (UobConstants.FIXED_FILE.equalsIgnoreCase(fileLayout)
				|| UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(fileLayout)) {
			for (int i = 0; i < referenceFileFieldList.size(); i++) {
				FieldModel fieldModel = referenceFileFieldList.get(i);
				String referenceFileName = fileLine.substring(fieldModel.getStartPosition(), fieldModel.getEndPosition());
				if (StringUtils.trimToNull(referenceFileName) != null) {
					fieldValues[i] = StringUtils.trim(referenceFileName);
				} else {
					fieldValues[i] = UobConstants.EMPTY;
				}
			}
		} else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(fileLayout)) {
			String escapedDelim = delim.replaceAll("\\|", "\\\\\\|");
			String[] fields = fileLine.split(escapedDelim, -1);
			for (int i = 0; i < referenceFileFieldList.size(); i++) {
				FieldModel fieldModel = referenceFileFieldList.get(i);
				String referenceFileName = fields[fieldModel.getFieldNum() - 1];
				if (StringUtils.trimToNull(referenceFileName) != null) {
					fieldValues[i] = StringUtils.trim(referenceFileName);
				} else {
					fieldValues[i] = UobConstants.EMPTY;
				}
			}
		}
		return fieldValues;
	}

	private Map<FieldModel, Integer> copyReferenceFilesToHDFSPath(String procId, String ctryCd, String businessDate,
			FileModel fileModel, Map<FieldModel, List<String>> fieldModelReferenceFileMap) throws EDAGIOException {
		logger.info(String.format("Copying reference files to HDFS Path %s ", fileModel.getHdfsTargetFolder()));
		Map<FieldModel, Integer> referenceFileCountMap = new HashMap<>();
		fieldModelReferenceFileMap.keySet().stream().forEach(fieldModel -> {
			referenceFileCountMap.put(fieldModel, 0);
		});
		// Deleting the contends of HDFS folder
		logger.info("Deleting the contents of the HDFS folder " + fileModel.getHdfsTargetFolder());
		Path p = new Path(fileModel.getHdfsTargetFolder());
		HadoopUtils.deleteHDFSFiles(p, Boolean.TRUE);
		boolean pathExist = HadoopUtils.checkIfFileExists(p);
		if (!pathExist) {
			logger.info("Recreating folder as path doesnt exist");
			HadoopUtils.createHDFSPath(p);
		}
		
		HashMap<String, FieldModel> refFieldMap = new HashMap<String, FieldModel>();
		String archivalFileDirectory = null;
		String archivalFileName = null;
		
		for (Map.Entry<FieldModel, List<String>> referenceFileMapEntries : fieldModelReferenceFileMap.entrySet()) {
			FieldModel fieldModel = referenceFileMapEntries.getKey();
			for (String referenceFileName : referenceFileMapEntries.getValue()) {
				boolean fileCopied = Boolean.FALSE;
				File referenceFile = new File(
						String.format("%s%s%s", fileModel.getReferenceFolderName(), File.separator, referenceFileName));
				if (referenceFile.exists()) {
					logger.debug("Copying file " + referenceFileName + " from landing area to "
							+ HadoopUtils.getConfigValue("fs.defaultFS") + fileModel.getReferenceFolderName());
					HadoopUtils.copyToHDFS(false, true, referenceFile,
							new Path(String.format("%s/%s", fileModel.getHdfsTargetFolder(), referenceFile.getName())));
					fileCopied = Boolean.TRUE;
				} else {
					refFieldMap.put(referenceFileName, fieldModel);
					fileCopied = Boolean.FALSE;
					archivalFileDirectory = fileModel.getSourceArchivalDir().substring(0,
							fileModel.getSourceArchivalDir().indexOf(fileModel.getSourceFileName()) - 1);
					archivalFileName = String.format("%s%s%s.%s.%s.%s", archivalFileDirectory, File.separator, procId,
							ctryCd, businessDate, "zip");
				}

				if(fileCopied) {
					referenceFileCountMap.put(fieldModel, referenceFileCountMap.get(fieldModel) + 1);
				}
			}
			
			if (archivalFileName != null) {
				checkIfFileExistsInArchive(refFieldMap, referenceFileCountMap, archivalFileDirectory, archivalFileName,
						fileModel);
			}
		}
		
		return referenceFileCountMap;
	}
	
	private boolean checkIfFileExistsInArchive(HashMap<String, FieldModel> refFieldMap,
			Map<FieldModel, Integer> referenceFileCountMap, String archivalFileDirectory, String archivalFileName,
			FileModel fileModel) throws EDAGIOException {

		logger.info("Check and Copy files exists in archival zip :- "+ archivalFileName);
		File zipFile = new File(archivalFileName);
		ZipInputStream zipStream = null;
		String entryName = "";
		FileSystem fs = null;
		int cnt = 0;
		if (zipFile.exists()) {
			try {
				fs = HadoopUtils.getFileSystem().get();
				zipStream = new ZipInputStream(new FileInputStream(zipFile));
				ZipEntry zipEntry = null;
				while ((zipEntry = zipStream.getNextEntry()) != null) {
					entryName = zipEntry.getName();
					if (refFieldMap.containsKey(entryName)) {
						logger.debug(String.format("Extracting the file %s from archival Zip File %s", entryName,
								archivalFileName));
						FSDataOutputStream hdfsfile = null;
						try {
							hdfsfile = fs.create(new Path(fileModel.getHdfsTargetFolder() + "/" + entryName));
							byte[] buffer = new byte[8192];
							int len;
							while ((len = zipStream.read(buffer)) != -1) {
								hdfsfile.write(buffer, 0, len);
							}
							logger.debug(
									"Created file in location:- " + fileModel.getHdfsTargetFolder() + "/" + entryName);
							FieldModel fieldModel = refFieldMap.get(entryName);
							referenceFileCountMap.put(fieldModel, referenceFileCountMap.get(fieldModel) + 1);
							cnt++;
						} catch (Exception e) {
							throw e;
						} finally {
							if (hdfsfile != null) {
								hdfsfile.flush();
								hdfsfile.close();
							}
						}
					}
				}
			} catch (Exception ie) {
				ie.printStackTrace();
				throw new EDAGIOException(EDAGIOException.CANNOT_EXTRACT_REFERENCE_ARCHIVE_FILE, ie, entryName,
						archivalFileName);
			} finally {
				logger.info("Total files copied from zip - "+ cnt);
				if (zipStream != null)
					try {
						zipStream.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
			}

		}

		return false;
	}

	

	private void writeToStageLog(ProcessInstanceModel procInstanceModel, ProcessModel processModel,
			Map<FieldModel, Integer> referenceFileCountMap, Map<FieldModel, Integer> copiedReferenceFilesCount, long totalRecords)
			throws EDAGMyBatisException {

		logger.info("Writing to process staging log");
		BigInteger referencedFileCount = new BigInteger("0");
		BigInteger referencedFileCopied = new BigInteger("0");
		BigInteger referencedFileMissing = new BigInteger("0");
		StageModel stageModel = new StageModel();
		stageModel.setStageId(UobConstants.STAGE_INGEST_T1_BDM_EXEC);
		stageModel.setProcInstanceId(procInstanceModel.getProcInstanceId());
		stageModel.setStartTime(new java.sql.Timestamp(System.currentTimeMillis()));
		stageModel.setEndTime(new java.sql.Timestamp(System.currentTimeMillis()));
		stageModel.setStatus(Status.I.toString());
		for (FieldModel fieldModel : referenceFileCountMap.keySet()) {
			int referenceFileCount = referenceFileCountMap.get(fieldModel);
			int copiedReferenceFileCount = copiedReferenceFilesCount.get(fieldModel);
			if (fieldModel.getDataType().equalsIgnoreCase("O")) {
				referencedFileCount = referencedFileCount.add(BigInteger.valueOf(referenceFileCount));
				referencedFileCopied = referencedFileCopied.add(BigInteger.valueOf(copiedReferenceFileCount));
			} else {
				referencedFileCount = referencedFileCount.add(BigInteger.valueOf(totalRecords));
				referencedFileCopied = referencedFileCopied.add(BigInteger.valueOf(copiedReferenceFileCount));
			}
			referencedFileMissing = referencedFileMissing.add(BigInteger.valueOf(totalRecords - copiedReferenceFileCount));
		}
		stageModel.setReferencedFileCount(referencedFileCount);
		stageModel.setReferencedFileCopied(referencedFileCopied);
		stageModel.setReferencedFileMissing(referencedFileMissing);
		stgHndle.addStageLogForAttachments(procInstanceModel, stageModel);
	}

	private void archiveReferenceFiles(String procId, String ctryCd, String businessDate, FileModel fileModel,
			Map<FieldModel, List<String>> fieldModelReferenceFileMap) throws EDAGIOException {

		String archivalFileDirectory = fileModel.getSourceArchivalDir().substring(0,
				fileModel.getSourceArchivalDir().indexOf(fileModel.getSourceFileName()) - 1);
		String archivalFileName = String.format("%s%s%s.%s.%s.%s", archivalFileDirectory, File.separator, procId, ctryCd, businessDate,
				"zip");
		logger.info(String.format("Archiving reference files to %s", archivalFileName));

		List<File> referenceFiles = new ArrayList<>();
		for (List<String> referenceFileList : fieldModelReferenceFileMap.values()) {
			for (String referenceFileName : referenceFileList) {
				File referenceFile = new File(
						String.format("%s%s%s", fileModel.getReferenceFolderName(), File.separator, referenceFileName));
				if (referenceFile.exists()) {
					referenceFiles.add(referenceFile);
				}
			}
		}

		File zipFile = new File(archivalFileName);
		byte[] buf = new byte[8192];
		ZipOutputStream zout;
		if (zipFile.exists()) {
			// Copy the existing zip entries into new entries
			try {
				File tempFile = File.createTempFile(zipFile.getName(), null);
				logger.debug(String.format("Temporary Zip File is %s", tempFile.getAbsolutePath()));
				tempFile.delete();
				FileUtils.moveFile(zipFile, tempFile);

				ZipInputStream zin = new ZipInputStream(new FileInputStream(tempFile));
				zout = new ZipOutputStream(new FileOutputStream(archivalFileName));
				ZipEntry entry = zin.getNextEntry();
				while (entry != null) {
					String name = entry.getName();
					boolean notInFiles = referenceFiles.stream().filter(f -> f.getName().equals(name)).count() == 0;
					if (notInFiles) {
						zout.putNextEntry(new ZipEntry(name));
						int len;
						while ((len = zin.read(buf)) > 0) {
							zout.write(buf, 0, len);
						}
					}
					entry = zin.getNextEntry();
				}
				zin.close();
				tempFile.delete();
			} catch (IOException ie) {
				throw new EDAGIOException(EDAGIOException.CANNOT_ARCHIVE_REFERENCE_FILES, ie, archivalFileName, procId, businessDate);
			}
		} else {
			try {
				zout = new ZipOutputStream(new FileOutputStream(archivalFileName));
			} catch (IOException ie) {
				throw new EDAGIOException(EDAGIOException.CANNOT_ARCHIVE_REFERENCE_FILES, ie, archivalFileName, procId, businessDate);
			}
		}
		try {
			// Add the new files to the stream
			for (File file : referenceFiles) {
				InputStream in = new FileInputStream(file);
				zout.putNextEntry(new ZipEntry(file.getName()));
				int len;
				while ((len = in.read(buf)) > 0) {
					zout.write(buf, 0, len);
				}
				zout.closeEntry();
				in.close();
				file.delete();
			}
		} catch (IOException ie) {
			throw new EDAGIOException(EDAGIOException.CANNOT_ARCHIVE_REFERENCE_FILES, ie, archivalFileName, procId, businessDate);
		} finally {
			if (zout != null) {
				try {
					zout.close();
				} catch (IOException ie) {
					throw new EDAGIOException("Error while closing zip file", ie);
				}
			}
		}
	}

	private void reconcileReferenceFileCountCheck(String procId, String ctryCd, String bizDate, long noOfRecords,
			List<FieldModel> referenceFileFieldList, Map<FieldModel, Integer> referenceFileCountMap,
			Map<FieldModel, Integer> copiedReferenceFileCount) throws EDAGProcessorException {
		logger.info(String.format("Reconciling reference file count for procID", procId));
		for (FieldModel referenceFileField : referenceFileFieldList) {
			String optionality = referenceFileField.getOptionality();
			int referenceFileCount = referenceFileCountMap.get(referenceFileField);
			int referenceFileCopiedCount = copiedReferenceFileCount.get(referenceFileField);
			if ("M".equalsIgnoreCase(optionality)) {
				if (referenceFileCount != noOfRecords || referenceFileCopiedCount == 0) {
					throw new EDAGProcessorException(EDAGProcessorException.REFERECE_FILE_COUNT_MISMATCH, 0,
							(noOfRecords - referenceFileCopiedCount), referenceFileField.getNormalizedFieldName());
				}
			} else if ("O".equalsIgnoreCase(optionality)) {
				// DO Nothing as the field is optional
			} else {
				Double percent = Double.parseDouble(optionality.toString());
				if (percent % 1 == 0) {
					long missingRecords = noOfRecords - referenceFileCopiedCount;
					if (missingRecords > percent.intValue()) {
						throw new EDAGProcessorException(EDAGProcessorException.REFERECE_FILE_COUNT_MISMATCH, percent.intValue(),
								missingRecords, referenceFileField.getNormalizedFieldName());
					}
				} else {
					double referenceFileMissingPercent = ((double) (noOfRecords - referenceFileCopiedCount) / noOfRecords) * 100;
					if (referenceFileMissingPercent > (percent * 100)) {
						throw new EDAGProcessorException(EDAGProcessorException.REFERECE_FILE_COUNT_MISMATCH,
								(int) (noOfRecords * percent), (noOfRecords - referenceFileCopiedCount),
								referenceFileField.getNormalizedFieldName());
					}
				}
			}
		}
	}

	private boolean checkIfFileExistsInArchive(String archivalFileName, String referenceFileName, String hdfsTargetFolder)
			throws EDAGIOException {
		File zipFile = new File(archivalFileName);
		if (zipFile.exists()) {
			try (ZipInputStream zipinputstream = new ZipInputStream(new FileInputStream(zipFile))) {
				ZipEntry entry = null;
				while ((entry = zipinputstream.getNextEntry()) != null) {
					if (entry.getName().equals(referenceFileName)) {
						logger.info(String.format("Extracting the file %s from archival Zip File %s", referenceFileName,
								archivalFileName));
						String extractedFilePath = extractEntry(entry, zipinputstream);
						File extractedFile = new File(extractedFilePath);
						logger.debug("Copying file " + extractedFilePath + " from landing area to "
								+ HadoopUtils.getConfigValue("fs.defaultFS") + hdfsTargetFolder);
						HadoopUtils.copyToHDFS(false, true, extractedFile,
								new Path(String.format("%s/%s", hdfsTargetFolder, referenceFileName)));
						extractedFile.delete();
						return Boolean.TRUE;
					}
				}
			} catch (IOException e) {
				throw new EDAGIOException(EDAGIOException.CANNOT_EXTRACT_REFERENCE_ARCHIVE_FILE, e, referenceFileName,
						archivalFileName);
			} catch (EDAGIOException ie) {
				throw new EDAGIOException(EDAGIOException.CANNOT_EXTRACT_REFERENCE_ARCHIVE_FILE, ie, referenceFileName,
						archivalFileName);
			}
		}
		return Boolean.FALSE;
	}

	private String extractEntry(final ZipEntry entry, InputStream is) throws EDAGIOException {
		try {
			java.nio.file.Path path = Files.createTempDirectory(String.valueOf(System.nanoTime()), new FileAttribute<?>[0]);
			String fileName = String.format("%s%s%s", path.toAbsolutePath(), File.separator, entry.getName());

			try (FileOutputStream fos = new FileOutputStream(fileName)) {
				final byte[] buf = new byte[8192];
				int length;
				while ((length = is.read(buf, 0, buf.length)) >= 0) {
					fos.write(buf, 0, length);
				}
			} catch (IOException ioex) {
				throw new EDAGIOException("Cannot extract from archival file", ioex);
			}
			return fileName;
		} catch (IOException ioex) {
			throw new EDAGIOException("Cannot extract from archival file", ioex);
		}
	}

	/*public static void main(String[] args) throws EDAGMyBatisException, EDAGValidationException {

		// String countryCode = args[0];
		// String archivalDir = args[1];
		// String referenceFolderName = args[2];
		// String hdfsTargetFolderName = args[3];
		// String optionality = args[4];
		// String bizDate = "2018-09-06";
		//
		// FileAttachmentProcessor fileIngestor = new FileAttachmentProcessor();
		// ProcessModel procModel = fileIngestor.retrieveMetadata("FI_PAL_ACCESSLOG_D01", countryCode);
		// procModel.getSrcInfo().setSourceArchivalDir(archivalDir);
		// procModel.getSrcInfo().setSourceFileLayoutCd(UobConstants.DELIMITED_FILE);
		//
		// // Setting Source Dir Path
		// FileModel fileModel = procModel.getSrcInfo();
		// String filePath = fileModel.getSourceDirectory();
		// if (StringUtils.isNotEmpty(filePath)) {
		// filePath = filePath.replace(UobConstants.COUNTRY_PARAM, countryCode.toLowerCase());
		// fileModel.setSourceDirectory(filePath);
		// }
		//
		// // Setting Archive Dir Path
		// String archFilePath = fileModel.getSourceArchivalDir();
		// if (StringUtils.isNotEmpty(archFilePath)) {
		// archFilePath = archFilePath.replace(UobConstants.COUNTRY_PARAM, countryCode.toLowerCase());
		// fileModel.setSourceArchivalDir(archFilePath);
		// }
		//
		// // Setting Control File Path
		// String ctrlFilePath = fileModel.getControlFileDir();
		// if (StringUtils.isNotEmpty(ctrlFilePath)) {
		// ctrlFilePath = ctrlFilePath.replace(UobConstants.COUNTRY_PARAM, countryCode.toLowerCase());
		// fileModel.setControlFileDir(ctrlFilePath);
		// }
		//
		// // Setting Staging Dir Path
		// DestModel destModel = procModel.getDestInfo();
		// String stagingDirName = destModel.getStagingDir();
		// if (StringUtils.isNotEmpty(stagingDirName)) {
		// stagingDirName = stagingDirName.replace(UobConstants.COUNTRY_PARAM, countryCode).replace(UobConstants.BIZ_DATE_PARAM,
		// bizDate);
		// destModel.setStagingDir(stagingDirName);
		// }
		//
		// // Setting Staging DB Name
		// String stagingDb = destModel.getStagingDbName();
		// if (StringUtils.isNotEmpty(stagingDb)) {
		// stagingDb = stagingDb.replace(UobConstants.COUNTRY_PARAM, countryCode);
		// destModel.setStagingDbName(stagingDb);
		// }
		//
		// // Setting Staging Partition
		// String stagingHivePart = destModel.getStagingHivePartition();
		// if (StringUtils.isNotEmpty(stagingHivePart)) {
		// String bizDatePartVal = "'" + bizDate + "'";
		// stagingHivePart = stagingHivePart.toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal);
		// destModel.setStagingHivePartition(stagingHivePart);
		// }
		//
		// // Setting DDS Partition
		// String hivePart = destModel.getHivePartition();
		// if (StringUtils.isNotEmpty(hivePart)) {
		// String ctryCdPartVal = "'" + countryCode + "'";
		// String bizDatePartVal = "'" + bizDate + "'";
		// String procInstanceIdPartVal = "'" + fileIngestor.procInstance.getProcInstanceId() + "'";
		// hivePart = hivePart.toLowerCase().trim().replace(UobConstants.SITE_ID_PARAM, ctryCdPartVal)
		// .replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal)
		// .replace(UobConstants.PROC_INSTANCE_ID_PARAM, procInstanceIdPartVal);
		// destModel.setHivePartition(hivePart);
		// }
		//
		// procModel.setSrcInfo(fileModel);
		// procModel.setDestInfo(destModel);
		//
		// FieldModel headerField = new FieldModel();
		// headerField.setRecordType(RecordType.HEADER);
		// headerField.setFieldName("Record Type");
		// headerField.setFieldDesc("Record Type");
		// headerField.setFieldNum(1);
		// headerField.setLength(1);
		// headerField.setDataType("A");
		// headerField.setDefaultValue("H");
		// headerField.setStartPosition(0);
		// headerField.setEndPosition(1);
		//
		// FieldModel footerField = new FieldModel();
		// footerField.setRecordType(RecordType.FOOTER);
		// footerField.setFieldName("Record Type");
		// footerField.setFieldDesc("Record Type");
		// footerField.setFieldNum(1);
		// footerField.setLength(1);
		// footerField.setDataType("A");
		// footerField.setDefaultValue("T");
		// footerField.setStartPosition(0);
		// footerField.setEndPosition(1);
		//
		// procModel.getSrcInfo().getSrcFieldInfo().add(headerField);
		// procModel.getSrcInfo().getSrcFieldInfo().add(footerField);
		//
		// for (FieldModel fieldModel : procModel.getSrcInfo().getSrcFieldInfo()) {
		// if (fieldModel.getFieldNum() == 5) {
		// fieldModel.setDataType(UobConstants.SRC_FILE_REFERENCE);
		// fieldModel.setOpt(optionality);
		// }
		// }
		//
		// procModel.getSrcInfo().setReferenceFolderName(referenceFolderName);
		// procModel.getSrcInfo().setHdfsTargetFolder(hdfsTargetFolderName);
		// fileIngestor.initIngestion(procModel, "2018-09-06", countryCode);

		// try {
		// fileIngestor.runFileIngestion(fileIngestor.procInstance, procModel, "2018-09-06", args[0], true, null);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		FileAttachmentProcessor fileIngestor = new FileAttachmentProcessor();
		ProcessModel procModel = fileIngestor.retrieveMetadata("FI_PAL_ACCESSLOG_D01", "SG");
		procModel.getSrcInfo().setSourceArchivalDir("/Users/bj186016/Code/ThinkBig/UOB/testFiles/ACCESSLOG.<biz_dt>");
		procModel.getSrcInfo().setSourceDirectory("/Users/bj186016/Code/ThinkBig/UOB/testFiles/ACCESSLOG");
		procModel.getSrcInfo().setSourceFileLayoutCd(UobConstants.DELIMITED_FILE);

		for (FieldModel fieldModel : procModel.getSrcInfo().getSrcFieldInfo()) {
			if (fieldModel.getFieldNum() == 5) {
				fieldModel.setDataType(UobConstants.SRC_FILE_REFERENCE);
				fieldModel.setOpt("M");
			}
		}

		ProcessParam processParam = new ProcessParam();
		processParam.setParamName("referencedfile.target.folder");
		processParam.setParamValue("/Users/bj186016/Code/ThinkBig/UOB/testFiles/${product}");
		procModel.getProcParam().add(processParam);

		processParam = new ProcessParam();
		processParam.setParamName("referencedfile.target.folder.product");
		processParam.setParamValue("color");
		procModel.getProcParam().add(processParam);

		processParam = new ProcessParam();
		processParam.setParamName("referencedfile.source.folder.product");
		processParam.setParamValue("color");
		procModel.getProcParam().add(processParam);

		processParam = new ProcessParam();
		processParam.setParamName("referencedfile.hdfs.uri.fieldname.suffix");
		processParam.setParamValue("color");
		procModel.getProcParam().add(processParam);

		procModel.getSrcInfo().setReferenceFolderName("/Users/bj186016/Code/ThinkBig/UOB/testFiles/source");
		procModel.getSrcInfo().setHdfsTargetFolder("/Users/bj186016/Code/ThinkBig/UOB/testFiles/target");

		ProcessInstanceModel procInstanceModel = new ProcessInstanceModel(procModel);
		procInstanceModel.setBizDate("2018-09-06");
		procInstanceModel.setCountryCd("SG");
		procInstanceModel.setProcInstanceId("FI_PAL_ACCESSLOG_D01");

		try {
			fileIngestor.runFileIngestion(procInstanceModel, procModel, "2018-09-06", "SG", true, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/
	
	public void checkAndCallIndexingProcess(ProcessInstanceModel procInstanceModel, ProcessModel processModel, String bizDate, String ctryCd) throws EDAGException{

//		1.hdfsInputPath
//		3.InputList - list of file names and collection names   map
//		4.regexLists - List of collection names and regex       map
//		5.procInstanceId
//		6.HiveRowCount  
		try {
		logger.info("** Going to start prepare parameter for spark job... **");
		
		String classParam = null;
		
		if (processModel!=null && processModel.getProcParam()!=null) {
			
			logger.info("** Check if Invoke prepareParamsForIndexing is started... **");
			
			// get hook class name
			for (int i=0; i < processModel.getProcParam().size(); i++) {
				ProcessParam paramObj = processModel.getProcParam().get(i);
				if (paramObj.getParamName().equalsIgnoreCase(UobConstants.UNS_INVOKE_CLASS_NAME)) {
					classParam = paramObj.getParamValue().trim();
					logger.info("** indexing hook class name is  **" + classParam);
					break;
				}
			}
			
			// if indexing class is exited.
			if (classParam != null) {
				
				HashMap<String, HashMap<String, String>> sparkParamMap = prepareParamsForIndexing(procInstanceModel, processModel, bizDate, ctryCd);
				logger.info("** Invoke prepareParamsForIndexing is done and returned value is okay. **");
				
				// Added by Tyler on 24th May 2019 for not invoke indexing if there is no available files to be indexed.
				HashMap<String, String> fileSolrCollectionMap = sparkParamMap.get(UobConstants.INPUTLIST);
				
				if (fileSolrCollectionMap.size() > 0) {

					Class cla = Class.forName(classParam);
					HookFrameworkInterface hfiObhj = (HookFrameworkInterface) cla.newInstance();
					logger.info("** Going to invoke the extract and index process ... **");

					Map<String, String> resultMap = hfiObhj.invokeExtractIndexProcess(sparkParamMap);
					logger.info("** returned result is : " + resultMap.get("UNS_PROCESS_RESULT") + "**");

					// Updated by Tyler on 24th May 2019 for drop T11 partition if solr process is failed as part of rollback logic.
					if (UobConstants.UNS_FAILED.equalsIgnoreCase(resultMap.get(UobConstants.UNS_PROCESS_RESULT))) {
						//dropHivePartition(processModel);
						throw new EDAGException("Solr Indexing is failed. Please check the log of solr indexing process.");
					}
				} else {
					logger.info("**No files to be indexing as there is no matched files between Hive data and Oracle meta data.**");
				}
				
			}else {
				logger.info("**No files to be indexing as there is no indexing class available in oracle meta table ( parameter table )**");
			}
		}
	  }
	  catch (ClassNotFoundException ce) {
			throw new EDAGIOException(EDAGIOException.CLASS_NOT_FOUND, ce.getMessage());
	  } catch (Exception e) {
			throw new EDAGIOException(e.getMessage());
	  }
	}
	
	public void setSeperateIndexingJobFlag(boolean flag) {
		this.seperateIndexingJob=flag;
	}
	
	public boolean getSeperateIndexingJobFlag() {
		return this.seperateIndexingJob;
	}
	
	/**
	 * This is the main class of any Indexing Process. This retrieves the metadata for the
	 * process id and calls the triggers the spark indexing job.
	 * @param arguments
	 * @throws EDAGException
	 */
	public static void main(String[] arguments) throws EDAGException {
	    boolean force = false;
	    String forceFileName = null;
	    CommandLineParser parser = new DefaultParser();
	    String hourToRun = null;
	   
	    try {
	      	      
	      options.addOption("h", "help", false, "Show Help");

	      Option forceOpt = new Option("f", "force", false, "Use this flag to force rerun the Ingestion Job from the Start");
	      forceOpt.setArgs(1);
	      forceOpt.setOptionalArg(true);
	      forceOpt.setArgName("File Location");
	      options.addOption(forceOpt);

	      Option inOpt = new Option("i", "ingest", false, "Run Ingestion Process for given Parameters");
	      inOpt.setArgs(3);
	      inOpt.setArgName("Process ID> <Biz Date> <Country Code");
	      options.addOption(inOpt);
	      
	      Option hourOpt = new Option("o", "hour", false, "Run Ingestion Process for specified hour");
	      hourOpt.setArgs(1);
	      inOpt.setArgName("hour option to run the ingestion process for the specified hour");
	      options.addOption(hourOpt);
	      

	      CommandLine command = parser.parse(options, arguments);

	      if (command.hasOption("h")) {
	        showHelp();
	      }

	      if (command.hasOption("f")) {
	        force = true;
	        forceFileName = command.getOptionValue("f");
	      }
	      
	      if(command.hasOption("o")) {
	    	  hourToRun = command.getOptionValue("o");
	      }
	      
	      if (!command.hasOption("i")) {
	        throw new EDAGProcessorException("Ingestion Option is mandatory");
	      }
	      
	      String[] args = command.getOptionValues("i");
	      if (args == null || args.length < 3) {
	        throw new EDAGProcessorException("Not enough arguments passed to run Ingestion");
	      }

	     	      
	      DateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
	      String execDate = formatter.format(System.currentTimeMillis());
	      String procId = args[0];
	      String bizDate = args[1];
	      try {
					new SimpleDateFormat("yyyy-MM-dd").parse(bizDate);
				} catch (java.text.ParseException e) {
					throw new EDAGValidationException(EDAGValidationException.INVALID_DATE_FORMAT, bizDate, "yyyy-MM-dd", e.getMessage());
				}
	      
	      String ctryCd = args[2];
	      String logFileName = "EDA_FI_" + ctryCd + "_DL_" + procId + "_" + bizDate + "_" + execDate + ".log";
	      System.setProperty("logFileName", logFileName);
	      staticLogger = Logger.getLogger(BaseProcessor.class);
	      UobUtils.logJavaProperties();
	      UobUtils.logPackageProperties();
	      staticLogger.info("Indexing Process Starts: " + execDate);
	      staticLogger.info(String.format("Arguments: Proc ID: %s, bizDate: %s, ctryCd: %s, hourToRun: %s, " ,  
	      		procId, bizDate, ctryCd, StringUtils.defaultString(hourToRun)));
	      
	      // monitor memory usage if applicable
		  IngestionDao ingestDao = new IngestionDao();
	      MemoryLogger memLogger = new MemoryLogger();
	      new Thread(memLogger).start();
	      BaseProcessor bp = new BaseProcessor();
	      staticLogger.info("Getting processMode for Proc Id: " + procId);
	      ProcessModel procModel = bp.retrieveMetadata(procId, ctryCd);
	      
	      staticLogger.info("Getting proc Instance Model for Proc Id: "+procId + " bizDate: "+bizDate + " ctryCd: "+ctryCd+" srcSysCd: "+ procModel.getSrcSysCd());
	      ProcessInstanceModel procInstanceModel = ingestDao.retrieveProcInstanceModelForIndexing(procId, bizDate, ctryCd, procModel.getSrcSysCd());
	      
	      FileAttachmentProcessor fileAttachProcess = new FileAttachmentProcessor();
	      fileAttachProcess.setSeperateIndexingJobFlag(Boolean.TRUE);
	      fileAttachProcess.setAttachmentInformation(procModel, ctryCd, bizDate);
	      staticLogger.info("Going to start indexing parameters : (" + procInstanceModel + " " + procModel + " " + bizDate + " " + ctryCd + ") ");
	      fileAttachProcess.checkAndCallIndexingProcess(procInstanceModel, procModel, bizDate, ctryCd);
	      
	      //staticLogger.info("Base Processor Completed: " + formatter.format(System.currentTimeMillis()));
	    } catch (ParseException excp) {
	      throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, excp.getMessage());
	    }
	  }

}
