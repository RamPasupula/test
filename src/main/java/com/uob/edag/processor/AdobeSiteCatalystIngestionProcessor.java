package com.uob.edag.processor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;

import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.AdobeProcessMapModel;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.dao.ImpalaDao;
import com.uob.edag.dao.IngestionDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;

public class AdobeSiteCatalystIngestionProcessor extends BaseProcessor implements IngestionProcessor {
	private final int BUFFER_SIZE = 4096;
	private String environment = null;
	private String environmentNum = null;

	private boolean dropHivePartitionOnReconciliationFailure = true;

	private FileUtility fileUtils;
	private int noOfRecords = 0;
	private List<String> consumedTarfilesList = new ArrayList<String>();
	private List<String> untarredfilesList = new ArrayList<String>();
	private List<String> tempUntarredfilesList = new ArrayList<String>();
	private String dataFileName;
	private double fileSize;
	private String md5Value;
	private int recordCount;
	private int destRecordCount;
	private int errorRecordCount;
	private ProcessInstanceModel procInstanceId;
	private HashMap<String,String> procInstIdMap;
	private List<String> procInstIdList;
	private List<String> successProcInstIdList;
	private List<String> failProcInstIdList;
	private String errorMessage;
	private boolean isMD5SumValidationFailure = Boolean.FALSE;
	protected boolean isBizDateValidationDisabled = Boolean.FALSE;
	protected boolean isRowCountReconciliationDisabled = Boolean.FALSE;
	protected boolean isHashSumReconciliationDisabled = Boolean.FALSE;
	protected boolean isMD5SumValidationDisabled = Boolean.FALSE;
	protected boolean isRowCountValidationDisabled = Boolean.FALSE;
	

	public static void main(String[] args) throws EDAGException {
		AdobeSiteCatalystIngestionProcessor asc = new AdobeSiteCatalystIngestionProcessor();
		asc.readReconcilationFile(args[0], args[1]);
		if (asc.getMD5ForFile()) {
			asc.extractTarGZ(args[1]);
		} else {
			// MD5 value did not match and so the file is corrupt. Will not proceed with the tar extraction.
			throw new EDAGIOException("MD5 value specified in the control file does not match with the tar file provided "
					+ "and hence extraction wont be performed.");
		}
	}

	public void runIngestion(String procId, String bizDate, String ctryCd, boolean force, String forceFileName) throws EDAGException {
		String finalSrcDir = "";
		String ctrlSrcFileWithPath = "";
		AdobeSiteCatalystIngestionProcessor asc = new AdobeSiteCatalystIngestionProcessor();
		try {
			String newBizDt = "";
			Date formatDate = new Date();

			procInstIdList = new ArrayList<String>();
			successProcInstIdList = new ArrayList<String>();
			failProcInstIdList = new ArrayList<String>();
			errorMessage = "";

			try {
				formatDate = new SimpleDateFormat("yyyy-MM-dd").parse(bizDate);
				newBizDt = new SimpleDateFormat("dd-MMM-yy").format(formatDate);
			} catch (Exception e) {
				throw e;
			}
			String tempProcIdList = ingestDao.retrieveAdobeProcSubProcMap(procId);
			String[] procIdList = tempProcIdList.split(",");
			ctrlSrcFileWithPath = ingestDao.retrieveSourceDirectory(procIdList[0]);
			ctrlSrcFileWithPath = ctrlSrcFileWithPath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
			ctrlSrcFileWithPath = ctrlSrcFileWithPath + "_" + bizDate + ".txt";
			for (int i = 0; i < ctrlSrcFileWithPath.split("/").length - 1; i++) {
				finalSrcDir = finalSrcDir + ctrlSrcFileWithPath.split("/")[i];
				if (i != ctrlSrcFileWithPath.split("/").length - 2)
					finalSrcDir += "/";
			}

			// ctrlSrcFileWithPath = ctrlSrcFileWithPath.replaceAll("/", "\\\\");
			// finalSrcDir = finalSrcDir.replaceAll("/", "\\\\");
			logger.info("Control File Dir: " + ctrlSrcFileWithPath);
			logger.info("Final Src Dir : " + finalSrcDir);

			asc.readReconcilationFile(ctrlSrcFileWithPath, finalSrcDir);
			if (asc.getMD5ForFile()) {
				asc.extractTarGZ(finalSrcDir);
			} else {
				if (this.isMD5SumValidationDisabled) {
					logger.info("Error not thrown as MD5 Sum validation is enabled");
					isMD5SumValidationFailure = Boolean.TRUE;
					asc.extractTarGZ(finalSrcDir);
				} else {
					// MD5 value did not match and so the file is corrupt. Will not proceed with the tar extraction.
					throw new EDAGIOException(
							"Either the MD5 value or the File-Size specified in the control file does not match with the tar file provided "
									+ "and hence extraction wont be performed.");
				}
			}
			procInstIdMap = new HashMap<String, String>();
			for (String tempProcId : procIdList) {
				BaseProcessor ingProc = new BaseProcessor();
				ingProc.isBizDateValidationDisabled = this.isBizDateValidationDisabled;
				ingProc.isRowCountReconciliationDisabled = this.isRowCountReconciliationDisabled;
				ingProc.isHashSumReconciliationDisabled = this.isHashSumReconciliationDisabled;
				ingProc.isRowCountValidationDisabled = this.isRowCountReconciliationDisabled;
				ingProc.isMD5SumValidationDisabled = this.isMD5SumValidationDisabled;
				try {
					ingProc.runIngestion(tempProcId, bizDate, ctryCd, force, forceFileName);
					if (tempProcId.equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_HIT_DATA_PROC) 
						|| tempProcId.equalsIgnoreCase(UobConstants.ADOBE_SITE_CATALYST_HIT_DATA_PROC_HISTORY))
						procInstanceId = ingProc.procInstance;
					if(this.isMD5SumValidationDisabled && this.isMD5SumValidationFailure) {
						logger.info("MD5 Sum validation is enabled");
						ingProc.procInstance.setMd5SumValidationMessage(UobConstants.MD5_SUM_VALIATION_MESSAGE);
					}

					procInstIdMap.put(tempProcId, ingProc.procInstance.getProcInstanceId());
					// Source Row Count Calculation
					ProcessModel procModel = retrieveMetadata(tempProcId, ctryCd);
					DestModel destModel = procModel.getDestInfo();
					boolean useImpala = false;
					String queueName = useImpala ? null : destModel.getHadoopQueueName();
					HiveDao hiveDao = useImpala ? new ImpalaDao() : new HiveDao();
					String stagingDb = destModel.getStagingDbName();
					if (StringUtils.isNotEmpty(stagingDb)) {
						stagingDb = stagingDb.replace(UobConstants.COUNTRY_PARAM, ctryCd);
					}
					String stagingHivePart = destModel.getStagingHivePartition();
					if (StringUtils.isNotEmpty(stagingHivePart)) {
						String bizDatePartVal = "'" + bizDate + "'";
						stagingHivePart = stagingHivePart.toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal);
					}
					int srcRowCount = hiveDao.getRowCountByPartition(stagingDb, destModel.getStagingTableName(), stagingHivePart,
							queueName);

					int destRowCount = hiveDao.getRowCount(destModel.getHiveDbName(), destModel.getHiveTableName(), ctryCd, bizDate,
							ingProc.procInstance.getProcInstanceId(), queueName);

					stgHndle.updateFinalStageLogForAdobe(ingProc.procInstance, UobConstants.STAGE_INGEST_PROC_FINAL, srcRowCount,
							destRowCount, destModel.getHiveDbName(), destModel.getHiveTableName(),
							destModel.getStagingErrorTableName());
					
					ingestDao.updateProcessLog(ingProc.procInstance);
					// Source Row Count Calculation End
					successProcInstIdList.add(ingProc.procInstance.getProcInstanceId());
				} catch (EDAGException excp) {
					failProcInstIdList.add(ingProc.procInstance.getProcInstanceId());
					if (errorMessage.length() > 0)
						errorMessage = errorMessage + ",";
					errorMessage = errorMessage + ingProc.procInstance.getProcInstanceId();
					logger.info("Process Id " + tempProcId + " failed : " + excp);
					continue;
				} finally {
					procInstIdList.add(ingProc.procInstance.getProcInstanceId());
				}
			}
			errorMessage = "Initially Successful but marked as failed due to the failure of Proc Instance Id : " + errorMessage;
			logger.info("Error Message : " + errorMessage);
			logger.info("successProcInstIdList : " + successProcInstIdList.size());
			logger.info("failProcInstIdList : " + failProcInstIdList.size());
			logger.info("procInstIdList : " + procInstIdList.size());
			String failedProcCount = ingestDao.getAdobeProcStatus(procInstIdList);
			if (!failedProcCount.equalsIgnoreCase("0")) {
				if (!successProcInstIdList.isEmpty())
					ingestDao.updateFinalProcessLog(UobConstants.FAILURE, errorMessage, successProcInstIdList);

				for (String tempProcId : procIdList) {
					FileIngestionProcessor ingProc = new FileIngestionProcessor();
					ProcessModel procModel = retrieveMetadata(tempProcId, ctryCd);
					// Set Variables

					// Setting Source Dir Path
					FileModel fileModel = procModel.getSrcInfo();
					String filePath = fileModel.getSourceDirectory();
					if (StringUtils.isNotEmpty(filePath)) {
						filePath = filePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
						// For Adobe Site Catalyst Ingestion
						filePath = filePath + "." + fileModel.getSourceFileExtn();
						fileModel.setSourceDirectory(filePath);
					}

					// Setting Archive Dir Path
					String archFilePath = fileModel.getSourceArchivalDir();
					if (StringUtils.isNotEmpty(archFilePath)) {
						archFilePath = archFilePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
						fileModel.setSourceArchivalDir(archFilePath);
					}

					// Setting Control File Path
					String ctrlFilePath = fileModel.getControlFileDir();
					if (StringUtils.isNotEmpty(ctrlFilePath)) {
						ctrlFilePath = ctrlFilePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
						logger.info("Control File :" + ctrlFilePath);
						fileModel.setControlFileDir(ctrlFilePath);
					}

					// Setting Staging Dir Path
					DestModel destModel = procModel.getDestInfo();
					String stagingDirName = destModel.getStagingDir();
					if (StringUtils.isNotEmpty(stagingDirName)) {
						stagingDirName = stagingDirName.replace(UobConstants.COUNTRY_PARAM, ctryCd)
								.replace(UobConstants.BIZ_DATE_PARAM, bizDate);
						logger.info("Staging Dir :" + stagingDirName);
						destModel.setStagingDir(stagingDirName);
					}

					// Setting Staging DB Name
					String stagingDb = destModel.getStagingDbName();
					if (StringUtils.isNotEmpty(stagingDb)) {
						stagingDb = stagingDb.replace(UobConstants.COUNTRY_PARAM, ctryCd);
						logger.info("Staging Db :" + stagingDb);
						destModel.setStagingDbName(stagingDb);
					}

					// Setting Staging Partition
					String stagingHivePart = destModel.getStagingHivePartition();
					if (StringUtils.isNotEmpty(stagingHivePart)) {
						String bizDatePartVal = "'" + bizDate + "'";
						stagingHivePart = stagingHivePart.toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal);
						logger.info("Staging Hive :" + stagingHivePart);
						destModel.setStagingHivePartition(stagingHivePart);
					}

					// Setting DDS Partition
					String hivePart = destModel.getHivePartition();
					if (StringUtils.isNotEmpty(hivePart)) {
						String ctryCdPartVal = "'" + ctryCd + "'";
						String bizDatePartVal = "'" + bizDate + "'";
						String procInstanceIdPartVal = "'" + procInstIdMap.get(procModel.getProcId()) + "'";
						hivePart = hivePart.toLowerCase().trim().replace(UobConstants.SITE_ID_PARAM, ctryCdPartVal)
								.replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal)
								.replace(UobConstants.PROC_INSTANCE_ID_PARAM, procInstanceIdPartVal);
						logger.info("Hive Part:" + hivePart);
						destModel.setHivePartition(hivePart);
					}

				        procModel.setSrcInfo(fileModel);
				        procModel.setDestInfo(destModel);
					    ingProc.dropHivePartition(procModel);
				   }
				   throw new EDAGException(errorMessage);
			   }

			   else {
				   ProcessModel procModel;
				   if(procId.endsWith("_H01"))
					   procModel = retrieveMetadata(UobConstants.ADOBE_SITE_CATALYST_HIT_DATA_PROC_HISTORY, ctryCd);
				   else
					   procModel = retrieveMetadata(UobConstants.ADOBE_SITE_CATALYST_HIT_DATA_PROC, ctryCd);
				   if(!runRowCountValidation(procModel, ctryCd, bizDate,asc))
				   {
					if(!this.isRowCountReconciliationDisabled) {   
					for(String tempProcId : procIdList) {
						   FileIngestionProcessor ingProc = new FileIngestionProcessor();
						   ProcessModel tempProcModel = retrieveMetadata(tempProcId, ctryCd);
						// Set Variables

							// Setting Source Dir Path
							FileModel fileModel = tempProcModel.getSrcInfo();
							String filePath = fileModel.getSourceDirectory();
							if (StringUtils.isNotEmpty(filePath)) {
								filePath = filePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
								// For Adobe Site Catalyst Ingestion
								filePath = filePath + "." + fileModel.getSourceFileExtn();
								fileModel.setSourceDirectory(filePath);
							}

							// Setting Archive Dir Path
							String archFilePath = fileModel.getSourceArchivalDir();
							if (StringUtils.isNotEmpty(archFilePath)) {
								archFilePath = archFilePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
								fileModel.setSourceArchivalDir(archFilePath);
							}

							// Setting Control File Path
							String ctrlFilePath = fileModel.getControlFileDir();
							if (StringUtils.isNotEmpty(ctrlFilePath)) {
								ctrlFilePath = ctrlFilePath.replace(UobConstants.COUNTRY_PARAM, ctryCd.toLowerCase());
								logger.info("Control File :" + ctrlFilePath);
								fileModel.setControlFileDir(ctrlFilePath);
							}

							// Setting Staging Dir Path
							DestModel destModel = tempProcModel.getDestInfo();
							String stagingDirName = destModel.getStagingDir();
							if (StringUtils.isNotEmpty(stagingDirName)) {
								stagingDirName = stagingDirName.replace(UobConstants.COUNTRY_PARAM, ctryCd)
										.replace(UobConstants.BIZ_DATE_PARAM, bizDate);
								logger.info("Staging Dir :" + stagingDirName);
								destModel.setStagingDir(stagingDirName);
							}

							// Setting Staging DB Name
							String stagingDb = destModel.getStagingDbName();
							if (StringUtils.isNotEmpty(stagingDb)) {
								stagingDb = stagingDb.replace(UobConstants.COUNTRY_PARAM, ctryCd);
								logger.info("Staging Db :" + stagingDb);
								destModel.setStagingDbName(stagingDb);
							}

							// Setting Staging Partition
							String stagingHivePart = destModel.getStagingHivePartition();
							if (StringUtils.isNotEmpty(stagingHivePart)) {
								String bizDatePartVal = "'" + bizDate + "'";
								stagingHivePart = stagingHivePart.toLowerCase().replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal);
								logger.info("Staging Hive :" + stagingHivePart);
								destModel.setStagingHivePartition(stagingHivePart);
							}

							// Setting DDS Partition
							String hivePart = destModel.getHivePartition();
							if (StringUtils.isNotEmpty(hivePart)) {
								String ctryCdPartVal = "'" + ctryCd + "'";
								String bizDatePartVal = "'" + bizDate + "'";
								String procInstanceIdPartVal = "'" + procInstIdMap.get(tempProcModel.getProcId()) + "'";
								hivePart = hivePart.toLowerCase().trim().replace(UobConstants.SITE_ID_PARAM, ctryCdPartVal)
										.replace(UobConstants.BIZ_DATE_PARAM, bizDatePartVal)
										.replace(UobConstants.PROC_INSTANCE_ID_PARAM, procInstanceIdPartVal);
								logger.info("Hive Part:" + hivePart);
								destModel.setHivePartition(hivePart);
							}

							tempProcModel.setSrcInfo(fileModel);
							tempProcModel.setDestInfo(destModel);
							ingProc.dropHivePartition(tempProcModel);
						}
						
						for(String procInsId : procInstIdList) {
							ProcessInstanceModel procInstanceModel = ingestDao.getProcessInstanceModel(procInsId);
							procInstanceModel.setStatus(UobConstants.FAILURE);
							procInstanceModel.setErrorTxt(String.format("Row count reconciliation failed for HIT Data with process instance ID %s, "
									+ "source row count is %d, target row count is %d, error count is %d",
								procInstanceId.getProcInstanceId(), asc.recordCount, asc.destRecordCount, asc.errorRecordCount));
							ingestDao.updateProcessLog(procInstanceModel);
							stgHndle.updateStageLog(procInsId, String.format("Row count reconciliation failed for HIT Data with process instance ID %s, "
									+ "source row count is %d, target row count is %d, error count is %d",
									procInstanceId, asc.recordCount, asc.destRecordCount, asc.errorRecordCount), UobConstants.FAILURE, UobConstants.STAGE_INGEST_PROC_FINAL);
						}
						throw new EDAGValidationException(EDAGValidationException.ROW_COUNT_RECONCILIATION_FAILURE,
								procInstanceId.getProcInstanceId(), asc.recordCount, asc.destRecordCount, asc.errorRecordCount);
					} else {
						logger.info("Row Count Reconciliation check is disabled");
						for(String procInstanceId : procInstIdList) {
							ProcessInstanceModel procInstanceModel = ingestDao.getProcessInstanceModel(procInstanceId);
							if(procInstanceModel.getStatus().equalsIgnoreCase(UobConstants.SUCCESS)) {
								if(this.isMD5SumValidationDisabled && this.isMD5SumValidationFailure) {
									procInstanceModel.setMd5SumValidationMessage(UobConstants.MD5_SUM_VALIATION_MESSAGE);
								}
								procInstanceModel.setRowCountReconciliationMessage(UobConstants.ROW_COUNT_RECONCILIATION_MESSAGE);
								procInstanceModel.setErrorTxt(procInstanceModel.getSuppressionMessage());
							}
							ingestDao.updateProcessLog(procInstanceModel);
							stgHndle.updateStageLog(procInstanceId, procInstanceModel.getSuppressionMessage(), procInstanceModel.getStatus(), UobConstants.STAGE_INGEST_PROC_FINAL);
						}
					}
				}
			}
		} catch (Throwable excp) {
			excp.printStackTrace();
			logger.info("Adobe file failed to ingest : " + excp.getMessage());
			EDAGException ex = new EDAGException(excp.getMessage(), excp);
			throw ex;
		} finally {
			archiveFile(asc.dataFileName, asc.dataFileName, false, bizDate, ctrlSrcFileWithPath);
		}
	}

	public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel procModel, String bizDate, String ctryCd,
			boolean forceRerun, String forceFileName) throws EDAGException {
		logger.info("Going to run File Ingestion for process: " + procModel.getProcId());
	}

	private void readReconcilationFile(String sourceFileNameWithPath, String processingfolder) throws EDAGIOException {
		Scanner scanner = null;
		try {
			scanner = new Scanner(new FileReader(sourceFileNameWithPath));
			String line;
			while (scanner.hasNextLine()) {
				line = scanner.nextLine();
				if (line.startsWith("Data-File")) {
					this.dataFileName = processingfolder + "/" + line.split(":")[1].trim();
				}
				if (line.startsWith("MD5-Digest")) {
					this.md5Value = line.split(":")[1].trim();
				}
				if (line.startsWith("Record-Count")) {
					this.recordCount = Integer.parseInt(line.split(":")[1].trim());
				}
				if (line.startsWith("File-Size")) {
					this.fileSize = Double.parseDouble(line.split(":")[1].trim());
				}
			}
		} catch (IOException e) {
			// output the failed files to log file.
			logger.info(
					"readControlFile failure: Unable to  read the control txt file " + sourceFileNameWithPath + "; " + e.getMessage());
			throw new EDAGIOException("Unable to read control txt file " + sourceFileNameWithPath + "; " + e.getMessage(), e);
		} finally {
			if (scanner != null)
				scanner.close();
		}
	}

	private String extractTarGZ(String processingfolder) throws EDAGIOException {
		String newSourceFileName = processingfolder + "/";
		File file = null;
		TarArchiveInputStream tarIn = null;
		GzipCompressorInputStream gzipIn = null;
		FileInputStream is = null;
		try {
			file = new File(this.dataFileName);
			is = new FileInputStream(file);
			gzipIn = new GzipCompressorInputStream(is);
			tarIn = new TarArchiveInputStream(gzipIn);
			TarArchiveEntry entry;

			while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
				/** If the entry is a directory, skip. **/
				if (entry.isDirectory()) {
					continue;
				}
				int count;
				byte data[] = new byte[BUFFER_SIZE];
				/** Create the target file and start copying the data **/
				File targetFile = new File(newSourceFileName + entry.getName());
				targetFile.createNewFile();
				FileOutputStream fos = new FileOutputStream(targetFile);
				try (BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER_SIZE)) {
					while ((count = tarIn.read(data, 0, BUFFER_SIZE)) != -1) {
						dest.write(data, 0, count);
					}
				} finally {
					if (fos != null) {
						fos.close();
					}
				}
			}
		} catch (IOException e) {

			// output the failed files to log file.
			logger.info("extractTarSourceFile failure: Unable to  extract tar " + this.dataFileName + " to " + processingfolder + "; "
					+ e.getMessage());
			throw new EDAGIOException("Unable to extract tar " + this.dataFileName + " to " + processingfolder + "; " + e.getMessage(),
					e);

		} finally {
			if (tarIn != null) {
				try {
					is.close();
					gzipIn.close();
					tarIn.close();
				} catch (IOException e) {

					// output the error message to log files.
					logger.info("extractTarSourceFileToProcessFolder: tar file is null. " + e.getMessage());
					throw new EDAGIOException(
							"Unable to extract tar " + this.dataFileName + " to " + processingfolder + ": " + e.getMessage(), e);
				}
			}
		}
		return newSourceFileName;
	}

	private boolean getMD5ForFile() throws EDAGIOException {
		String md5Value = "";
		boolean result = false;
		FileInputStream fs = null;
		try {
			File file = new File(this.dataFileName);
			double l = file.length();
			fs = new FileInputStream(file);
			MessageDigest md;
			try {
				md = MessageDigest.getInstance("MD5");
				byte[] dataBytesArr = new byte[1024];
				int numread;
				while ((numread = fs.read(dataBytesArr)) != -1) {
					md.update(dataBytesArr, 0, numread);
				}
				byte[] hashValue = md.digest();
				md5Value = toHexString(hashValue);

			} catch (NoSuchAlgorithmException | FileNotFoundException e) {
				e.printStackTrace();
			}
			// logger.debug("Calculated MD5value: "+ md5Value);
			// logger.debug("Original MD5value: "+ this.md5Value);
			// logger.debug("Calculated file length: "+ Math.ceil(l));
			// logger.debug("original file length: "+ this.fileSize);
			if ((md5Value.equals(this.md5Value)) && (Math.ceil(l) == this.fileSize))
				result = true;
		} catch (IOException e) {
			// output the error message to log files.
			logger.info("getControlDetailsForFile: Unable to read MD5 value or File-Size from control txt file ; " + e.getMessage());
			throw new EDAGIOException("Unable to read MD5 value or File-Size from control txt file ;" + e.getMessage(), e);
		} finally {
			IOUtils.closeQuietly(fs);
		}
		return result;
	}

	private String toHexString(byte[] bytes) {
		char[] hexArray = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
		char[] hexChars = new char[bytes.length * 2];
		int v;
		for (int j = 0; j < bytes.length; j++) {
			v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v / 16];
			hexChars[j * 2 + 1] = hexArray[v % 16];
		}
		return new String(hexChars);
	}

	public boolean runRowCountValidation(ProcessModel procModel, String countryCode, String bizDate,
			AdobeSiteCatalystIngestionProcessor asc) throws EDAGValidationException, EDAGMyBatisException {
		HadoopModel destModel = procModel.getDestInfo();
		boolean useImpala = false;
		// useImpala = UobUtils.parseBoolean(PropertyLoader.getProperty(com.uob.edag.utils.ReconciliationUtil.useImpala));
		// logger.info("Reconciliation will be done using " + (useImpala ? "Impala" : "Hive"));
		String queueName = useImpala ? null : destModel.getHadoopQueueName();

		// Get source row count
		int srcRowCount = asc.recordCount;

		// Get destination row count
		HiveDao hiveDao = useImpala ? new ImpalaDao() : new HiveDao();
		int destRowCount;

		destRowCount = hiveDao.getRowCount(destModel.getHiveDbName(), destModel.getHiveTableName(), countryCode, bizDate,
				procInstanceId.getProcInstanceId(), queueName);

		String stagingDb = destModel.getStagingDbName();
		if (StringUtils.isNotEmpty(stagingDb)) {
			stagingDb = stagingDb.replace(UobConstants.COUNTRY_PARAM, countryCode);
		}
		asc.errorRecordCount = hiveDao.getErrorRowCount(stagingDb, destModel.getStagingErrorTableName(), bizDate, queueName);

		asc.destRecordCount = destRowCount;

		logger.debug("Row Count Validation Successful for process ID " + procModel.getProcId() + ", source row count: " + srcRowCount
				+ ", target row count: " + destRowCount + ", error row count: " + asc.errorRecordCount);

		return validateRowCount(srcRowCount, destRowCount);
	}

	private boolean validateRowCount(int rowCount1, int rowCount2) {
		boolean result = rowCount1 == rowCount2;
		logger.info("Row count 1: " + rowCount1 + ", row count 2: " + rowCount2 + ", result: " + result);
		return result;
	}

	public void archiveFile(String sourceFilename, String targetFilename, boolean deleteSourceFile, String bizDt,
			String CtrlSrcFileWithPath) throws EDAGIOException {
		File source = new File(sourceFilename);
		File target = new File(targetFilename);
		File ctrlFile = new File(CtrlSrcFileWithPath);
		File targetDir = target.getParentFile();
		if (!targetDir.isDirectory()) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, targetDir.getPath(), "Directory doesnt' exist");
		}

		File targetFile = new File(target.getParentFile() + "/previous", target.getName());
		if (targetFile.isFile()) {
			File renameTarget = new File(targetFile.getParentFile(),
					targetFile.getName() + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
			try {
				Files.move(targetFile.toPath(), renameTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
				logger.info(targetFile.getPath() + " renamed to " + renameTarget.getPath());
			} catch (IOException e) {
				throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, targetFile.getPath(), renameTarget.getPath(),
						e.getMessage());
			}
		}
		File targetCtrlFile = new File(ctrlFile.getParentFile() + "/previous", ctrlFile.getName());
		if (targetCtrlFile.isFile()) {
			File renameTarget = new File(targetCtrlFile.getParentFile(),
					targetCtrlFile.getName() + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
			try {
				Files.move(targetCtrlFile.toPath(), renameTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
				logger.info(targetCtrlFile.getPath() + " renamed to " + renameTarget.getPath());
			} catch (IOException e) {
				throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, targetCtrlFile.getPath(), renameTarget.getPath(),
						e.getMessage());
			}
		}

		File renameTarget = new File(target.getParentFile() + "/previous", targetFile.getName() + "." + bizDt);
		targetFile = new File(target.getParentFile(), target.getName());
		try {
			Files.move(targetFile.toPath(), renameTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
			logger.info(targetFile.getPath() + " renamed to " + renameTarget.getPath());
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, targetFile.getPath(), renameTarget.getPath(), e.getMessage());
		}
		logger.info(targetFile.getPath() + " created");

		File renameTargetCtrlFile = new File(ctrlFile.getParentFile() + "/previous", ctrlFile.getName() + "." + bizDt);
		targetCtrlFile = new File(ctrlFile.getParentFile(), ctrlFile.getName());
		try {
			Files.move(targetCtrlFile.toPath(), renameTargetCtrlFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			logger.info(targetCtrlFile.getPath() + " renamed to " + renameTargetCtrlFile.getPath());
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, targetCtrlFile.getPath(), renameTargetCtrlFile.getPath(),
					e.getMessage());
		}
		logger.info(targetCtrlFile.getPath() + " created");

		if (deleteSourceFile) {
			if (source.delete()) {
				logger.info(source.getPath() + " deleted");
			} else {
				logger.warn(source.getPath() + " cannot be deleted, its file handle might be held by another process");
			}
		}
	}

}
