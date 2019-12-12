package com.uob.edag.processor;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.utils.HadoopUtils;

public class WebLogsIngestionProcessor extends FileIngestionProcessor implements IngestionProcessor {

	@Override
	public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel processModel, String bizDate, String ctryCd,
			boolean forceRerun, String forceFileName) throws EDAGException {

		logger.info("Going to run WebLog File Ingestion for process: " + processModel.getProcId());
		String topicName = "";
		String hdfsFolderPath = "";
		String loadTableFrequency = "";
		for (ProcessParam param : processModel.getProcParam()) {
			if (UobConstants.WEBLOGS_EDAG_LOAD_TABLE_FREQUENCY.equalsIgnoreCase(param.getParamName())) {
				loadTableFrequency = param.getParamValue();
			} else if (UobConstants.WEBLOGS_HDFS_FOLDER_PATH.equalsIgnoreCase(param.getParamName())) {
				hdfsFolderPath = param.getParamValue();
			} else if (UobConstants.WEBLOGS_TOPIC_NAME.equalsIgnoreCase(param.getParamName())) {
				topicName = param.getParamValue();
			}
		}

		FileModel fileModel = processModel.getSrcInfo();
		StringBuilder sb = new StringBuilder();
		sb.append(bizDate);
		try {
			logger.debug(String.format("The configured topic name for proc id %s is %s ", processModel.getProcId(), topicName));

			logger.info(String.format("Retriving all the hdfs files to load for process %s with hdfs folder location configured as %s",
					processModel.getProcId(), hdfsFolderPath));

			List<Path> hdfsFolderPathToScan = new ArrayList<>();
			if (loadTableFrequency.equalsIgnoreCase("D")) {
				procInstanceModel.setHourToRun(null);
				String hdfsSourcePathFormat = String.format("%s%s%s", hdfsFolderPath, "/", sb.toString());
				List<Path> hadoopPaths = HadoopUtils.listFiles(new Path(hdfsFolderPath), true, true);
				hdfsFolderPathToScan = hadoopPaths.stream().filter(f -> f.toUri().toString().contains(hdfsSourcePathFormat))
						.collect(Collectors.toList());
				hdfsFolderPathToScan.forEach(action -> logger.debug(String.format("HDFS File to load is %s", action)));
			} else {
				if (StringUtils.trimToNull(procInstanceModel.getHourToRun()) != null) {
					sb.append("-").append(procInstanceModel.getHourToRun());
					logger.debug(String.format("The HOUR to be run for Logs is configured as %s", procInstanceModel.getHourToRun()));
				} else {
					logger.info(String.format("The hour parameter is missing for %s", processModel.getProcId()));
					throw new EDAGProcessorException(EDAGProcessorException.MISSING_PROCESSING_HOUR_PARAM, processModel.getProcId());
				}

				Path hdfsSourcePath = new Path(String.format("%s%s%s", hdfsFolderPath, "/", sb.toString()));
				if (HadoopUtils.checkIfFileExists(hdfsSourcePath)) {
					logger.debug(String.format("Listing all the files with in the HDFS directory: %s", hdfsSourcePath));
					hdfsFolderPathToScan = HadoopUtils.listFiles(hdfsSourcePath, true, true);
					hdfsFolderPathToScan.forEach(action -> logger.debug(String.format("HDFS File to load is %s", action)));
				}
			}

			ProcessInstanceModel prevStatusModel = ingestDao.getPrevRunStatus(procInstanceModel);
			if (prevStatusModel != null && UobConstants.SUCCESS.equalsIgnoreCase(prevStatusModel.getStatus())) {
				logger.info(String.format(
						"Removing previous added partition %s as the previous run is successful and cleanup is required to remove the data",
						prevStatusModel.getProcInstanceId()));
				dropSuccessFulProcInstancePartition(prevStatusModel.getProcInstanceId(), procInstanceModel.getProcInstanceId(),
						processModel);
			}

			if (hdfsFolderPathToScan.isEmpty()) {
				logger.info(String.format("No Files to process for proc id %s. Hence exiting without processing the files.",
						processModel.getProcId()));
				stgHndle.addStageLog(procInstanceModel, UobConstants.STAGE_INGEST_PROC_INIT);
				stgHndle.updateStageLog(procInstanceModel, UobConstants.STAGE_INGEST_PROC_INIT, null);
				return;
			}

			List<FieldModel> filedModelList = processModel.getSrcInfo().getSrcFieldInfo().stream()
					.filter(f -> f.getRecordType().isData()).sorted((f1, f2) -> Integer.compare(f1.getFieldNum(), f2.getFieldNum()))
					.collect(Collectors.toList());

			CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator"));
			int noOfRecords = 0;

			try (BufferedWriter writer = new BufferedWriter(
					new OutputStreamWriter(new FileOutputStream(new File(fileModel.getSourceDirectory()))));
					CSVPrinter printer = new CSVPrinter(writer, format)) {
				for (Path path : hdfsFolderPathToScan) {
					if (HadoopUtils.checkIfFileExists(path)) {
						logger.info(String.format("Reading from HDFS file %s", path));
						BufferedReader fileReader = new BufferedReader(new InputStreamReader(HadoopUtils.openHDFSFile(path)));
						String line = null;
						while ((line = fileReader.readLine()) != null) {
							List<String> record = new ArrayList<>();
							for (FieldModel fieldModel : filedModelList) {
								String tempRegx = fieldModel.getFieldRegExpression();
								String field_value = UobConstants.EMPTY;
								Matcher macher = Pattern.compile(tempRegx, Pattern.CASE_INSENSITIVE).matcher(line);
								if (macher.find()) {
									int count = macher.groupCount();
									field_value = macher.group(count);
								}
								record.add(field_value);
							}
							printer.printRecord(record);
							noOfRecords++;
						}
						fileReader.close();
					}
				}
			}
			super.setNoOfRecords(noOfRecords);
			super.runFileIngestion(procInstanceModel, processModel, bizDate, ctryCd, forceRerun, forceFileName);
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
			throw new EDAGProcessorException(EDAGProcessorException.CANNOT_INGEST, processModel.getProcId(), e);
		}

	}

	private void dropSuccessFulProcInstancePartition(String prevProcInstanceId, String currProcInstanceId, ProcessModel processModel)
			throws EDAGMyBatisException {
		HiveDao dao = new HiveDao();
		HadoopModel destModel = processModel.getDestInfo();
		String schemaName = destModel.getHiveDbName();
		String tableName = destModel.getHiveTableName();
		String partitionInfo = destModel.getHivePartition();
		dao.dropHivePartition(schemaName, tableName, partitionInfo.replaceAll(currProcInstanceId, prevProcInstanceId));
	}

}
