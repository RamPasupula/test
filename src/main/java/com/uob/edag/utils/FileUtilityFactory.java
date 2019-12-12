package com.uob.edag.utils;

import org.apache.log4j.Logger;

import com.uob.edag.model.ProcessModel;
import com.uob.edag.utils.FileUtility.OperationType;

public class FileUtilityFactory {
	
	private static Logger logger = Logger.getLogger(FileUtilityFactory.class);
	
	public static FileUtility getFileUtility(ProcessModel procModel) {
		String operationType = PropertyLoader.getProperty(FileUtility.OperationType.class.getName());
		OperationType opType = operationType == null ? OperationType.Local : OperationType.valueOf(operationType);
		logger.info("File operations will be run " + (opType == OperationType.Local ? "locally" : "remotely"));
		return getFileUtility(procModel, opType);
	}
	
	public static FileUtility getFileUtility(ProcessModel procModel, OperationType opType) {
		return opType == OperationType.Local ? new FileUtils() : new RemoteFileUtils(procModel.getDeployNodeNm());
	}
}
