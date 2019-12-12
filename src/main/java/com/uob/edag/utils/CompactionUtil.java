package com.uob.edag.utils;

import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.HiveDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;

public class CompactionUtil {

	private static Logger logger = Logger.getLogger(CompactionUtil.class);

	private static String BIZ_DT_PARTION_COMPACTION_LOGIC_VALUE = "9999-12-31";

	private static final Format SALT_FORMATTER = new DecimalFormat("0000000000");

	private String createProcInstanceId(String procId, String bizDate, String ctryCd) {
		String result = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		String salt = SALT_FORMATTER.format((procId + "|" + bizDate + "|" + ctryCd).hashCode());
		return result + "." + salt;
	}

	public void performCompaction(ProcessInstanceModel procInstanceModel, ProcessModel processModel) throws EDAGException {
		logger.info(String.format("Performing Compaction for process id %s", processModel.getProcId()));
		String sourceTableName = String.format("%s.%s", processModel.getDestInfo().getHiveDbName(),
				processModel.getDestInfo().getHiveTableName());
		String targetTableName = UobConstants.EMPTY;
		String targetPartitionConfigValue = UobConstants.EMPTY;
		boolean dropSourceTablePartition = Boolean.FALSE;
		boolean droppedSourcePartition = Boolean.FALSE;

		for (ProcessParam procParam : processModel.getProcParam()) {
			if (UobConstants.TARGET_TABLE_NAME.equalsIgnoreCase(procParam.getParamName())) {
				targetTableName = procParam.getParamValue();
			}
			if (UobConstants.TARGET_TABLE_PARTITION_INFO.equalsIgnoreCase(procParam.getParamName())) {
				targetPartitionConfigValue = procParam.getParamValue();
			}
			if (UobConstants.DROP_SOURCE_TABLE_PARTITION.equalsIgnoreCase(procParam.getParamName())) {
				dropSourceTablePartition = Boolean.parseBoolean(procParam.getParamValue());
			}
		}

		if (StringUtils.trimToNull(targetTableName) == null) {
			targetTableName = String.format("%s.%s", processModel.getDestInfo().getHiveDbName(),
					processModel.getDestInfo().getHiveTableName());
		}

		if (StringUtils.trimToNull(targetPartitionConfigValue) != null) {
			targetPartitionConfigValue = processModel.getDestInfo().getTargetTablePartitionInfo();
		}

		if (!dropSourceTablePartition && targetTableName.equalsIgnoreCase(sourceTableName)) {
			dropSourceTablePartition = Boolean.TRUE;
		}

		String targetPartitionInfo = targetPartitionConfigValue.toLowerCase()
				.replace(UobConstants.SITE_ID_PARAM, String.format("'%s'", procInstanceModel.getCountryCd()))
				.replace(UobConstants.BIZ_DATE_PARAM, String.format("'%s'", BIZ_DT_PARTION_COMPACTION_LOGIC_VALUE))
				.replace(UobConstants.PROC_INSTANCE_ID_PARAM, String.format("'%s'", createProcInstanceId(processModel.getProcId(),
						procInstanceModel.getBizDate(), procInstanceModel.getCountryCd())));

		String sourcePartitionInfo = processModel.getDestInfo().getTargetTablePartitionInfo().toLowerCase()
				.replace(UobConstants.SITE_ID_PARAM, String.format("'%s'", procInstanceModel.getCountryCd()))
				.replace(UobConstants.BIZ_DATE_PARAM, String.format("'%s'", procInstanceModel.getBizDate()))
				.replace(UobConstants.PROC_INSTANCE_ID_PARAM, String.format("'%s'", createProcInstanceId(processModel.getProcId(),
						procInstanceModel.getBizDate(), procInstanceModel.getCountryCd())));

		String dropPartitionInfo = sourcePartitionInfo.substring(0, targetPartitionInfo.lastIndexOf(UobConstants.COMMA));

		StringBuilder sb = new StringBuilder();
		for (FieldModel fieldInfo : processModel.getSrcInfo().getSrcFieldInfo()) {
			sb.append(fieldInfo.getFieldName()).append(UobConstants.COMMA);
		}

		if (!sourcePartitionInfo.toLowerCase().contains(UobConstants.PROC_INSTANCE_ID.toLowerCase())) {
			sb.append(UobConstants.PROC_INSTANCE_ID);
			sb.append(UobConstants.COMMA);
		}

		sb.append(UobConstants.PROC_TIME);

		String sourceFileds = sb.substring(0, sb.length());

		sb = new StringBuilder();
		String[] partitions = dropPartitionInfo.split(UobConstants.COMMA);
		for (int i = 0; i < partitions.length - 1; i++) {
			sb.append(partitions[i]).append(UobConstants.COMMA);
		}

		String whereCondition = String.format("%s AND %s", sb.substring(0, sb.length() - 1), partitions[partitions.length - 1]);
		HiveDao hiveDAO = new HiveDao();

		try {
			List<Map<String, String>> partitionsInfoList = hiveDAO.getTablePartitions(sourceTableName);
			Map<String, String> sourceTablePartitions = Arrays.asList(dropPartitionInfo.split(UobConstants.COMMA)).stream()
					.map(f -> f.split(UobConstants.EQUAL, -1)).collect(Collectors.toMap(f -> f[0], f -> f[1].replaceAll("'", "")));
			List<Map<String, String>> filteredPartitionList = partitionsInfoList.stream().filter(f -> {
				boolean result = true;
				for (Map.Entry<String, String> entries : sourceTablePartitions.entrySet()) {
					String value = f.get(entries.getKey());
					if (value != null && value.replaceAll("'", "").equals(entries.getValue())) {
						continue;
					} else {
						result = false;
						break;
					}
				}
				return result;
			}).collect(Collectors.toList());

			if (filteredPartitionList.size() >= 2) {
				logger.info(String.format(
						"Compacting table %s and inserting date into partition %s, from source table %s with filter condition %s",
						targetTableName, targetPartitionInfo, sourceTableName, whereCondition));
				hiveDAO.compactPartition(sourceTableName, targetTableName, whereCondition, sourceFileds, targetPartitionInfo);
				if (dropSourceTablePartition) {
					logger.info(
							String.format("Dropping partition %s of source table %s", String.join(",", partitions), sourceTableName));
					hiveDAO.dropHivePartition(sourceTableName, String.join(",", partitions));
					droppedSourcePartition = Boolean.TRUE;
				}
				hiveDAO.renamePartition(targetTableName, targetPartitionInfo,
						targetPartitionInfo.replaceAll(BIZ_DT_PARTION_COMPACTION_LOGIC_VALUE, procInstanceModel.getBizDate()));
			}
		} catch (Exception e) {
			try {
				hiveDAO.dropHivePartition(targetTableName, targetPartitionInfo);
			} catch (Exception e1) {
				logger.error(ExceptionUtils.getFullStackTrace(e1));
			}
			logger.error(String.format("Exception while running compaction for process id ", processModel.getProcId()));
			logger.error(ExceptionUtils.getFullStackTrace(e));
			if (droppedSourcePartition) {
				throw new EDAGProcessorException(EDAGProcessorException.FAILURE_TO_PERFORM_COMPACTION, processModel.getProcId(),
						e.getMessage(), procInstanceModel.getBizDate());
			}
		}
	}

}