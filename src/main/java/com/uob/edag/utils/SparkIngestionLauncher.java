package com.uob.edag.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.IngestionDao;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.SparkExecParamsModel;

public class SparkIngestionLauncher {

	private static final Logger logger = Logger.getLogger(SparkIngestionLauncher.class);
	protected IngestionDao ingestDao = new IngestionDao();
	
	public String launchSparkApplication(ProcessInstanceModel procInstanceModel, ProcessModel procModel)
			throws EDAGProcessorException, EDAGIOException {

		logger.info(String.format("Launching spark application to ingest data into T1.1 layer for proc id %s",
				procInstanceModel.getProcId()));

		String sparkHome = PropertyLoader.getProperty(UobConstants.SPARK_HOME);
		String sparkMaster = PropertyLoader.getProperty(UobConstants.SPARK_MASTER);
		String sparkDeployMode = PropertyLoader.getProperty(UobConstants.SPARK_DEPLOY_MODE);
		String keytabLocation = PropertyLoader.getProperty(UobConstants.KEYTAB_LOCATION);
		String keytabName = PropertyLoader.getProperty(UobConstants.KEYTAB_NAME);
		String jaasConfigLocation = PropertyLoader.getProperty(UobConstants.JAAS_CONFIG_LOCATION);
		String applicationJarPath = PropertyLoader.getProperty(UobConstants.APPLICATION_JAR_PATH);
		String applicationMainClass = PropertyLoader.getProperty(UobConstants.APPLICATION_MAIN_CLASS_NAME);
		String applicationPropertyFile = PropertyLoader.getProperty(UobConstants.APPLICATION_PROPERTY_FILE_LOCATION);
		String additionalJars = PropertyLoader.getProperty(UobConstants.ADDITIONAL_JARS_CONFIG_NAME);

		if (!sparkDeployMode.equals(UobConstants.SPARK_DEPLOY_MODE_ACCEPTED)) {
			throw new EDAGProcessorException(EDAGProcessorException.INVALID_SPARK_DEPLOY_MODE, sparkDeployMode,
					procInstanceModel.getProcId());
		}

		List<String> applicationArgs = new ArrayList<>();
		applicationArgs.add("-p");
		applicationArgs.add(applicationPropertyFile);
		applicationArgs.add("-i");
		applicationArgs.add(procInstanceModel.getProcId());
		applicationArgs.add("-c");
		applicationArgs.add(procInstanceModel.getCountryCd());
		applicationArgs.add("-b");
		applicationArgs.add(procInstanceModel.getBizDate());
		applicationArgs.add("-o");
		applicationArgs.add(procInstanceModel.getProcInstanceId());

		Map<String, String> sparkDynamicProperties = getDynamicMemoryConfig(procModel.getDestInfo().getStagingDir(), procModel.getProcId(), procInstanceModel.getCountryCd());

		SparkLauncher launcher = new SparkLauncher().setSparkHome(sparkHome).setAppResource(applicationJarPath)
				.setAppName(String.format("Ingestion process for process ID %s", procInstanceModel.getProcId()))
				.setMainClass(applicationMainClass).setMaster(sparkMaster).setDeployMode(sparkDeployMode).setVerbose(true)
				.setConf("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=./jaas.conf")
				.setConf("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=./jaas.conf")
				.addSparkArg("--files", String.format("%s#%s,%s#%s", jaasConfigLocation, "jaas.conf", keytabLocation, keytabName))
				.addSparkArg("--queue", procModel.getDestInfo().getHadoopQueueName())
				.addAppArgs(applicationArgs.toArray(new String[0]));

		sparkDynamicProperties.forEach((key, value) -> {
			launcher.setConf(key, value);
		});

		for (String additionalJar : additionalJars.split(",")) {
			launcher.addJar(additionalJar);
		}

    try {
			SparkAppHandle appHandler = launcher.startApplication();
			while (!appHandler.getState().isFinal()) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				}
			}
			State appState = appHandler.getState();

			logger.info(String.format("Spark application is completed and its application id %s", appHandler.getAppId()));
			logger.info(String.format("Final State of the spark application is %s", appState));

			if (appState == State.FAILED || appState == State.KILLED || appState == State.LOST) {
				throw new EDAGProcessorException(EDAGProcessorException.SPARK_APPLICATION_FAILED, procInstanceModel.getProcId());
			}
			return appHandler.getAppId();
		} catch (IOException e) {
			logger.error("Exception occurred while launching spark application");
			logger.error(ExceptionUtils.getStackTrace(e));
			throw new EDAGIOException(EDAGIOException.SPARK_IO_EXCEPTION, procInstanceModel.getProcId());
		}
	}

	private Map<String, String> getDynamicMemoryConfig(String stagingHiveLocation, String procId, String ctryCd) throws EDAGIOException {
		long sizeInBytes = HadoopUtils.getSizeOfHDFSDirectoryPath(stagingHiveLocation);
		long sizeInMB = sizeInBytes / (1024 * 1024);
		Map<String, String> sparkDynamicProperties = new HashMap<>();
		
		logger.info("getting dynamic spark properties from Property File Info...");
		String driverMemoryPropValue = PropertyLoader.getProperty(UobConstants.SPARK_DRIVER_MEMORY);
		String executorMemoryPropValue = PropertyLoader.getProperty(UobConstants.SPARK_EXECUTOR_MEMORY);
		String executorInstancePropValue = PropertyLoader.getProperty(UobConstants.SPARK_EXECUTOR_INSTANCE);
		String executorCoresPropValue = PropertyLoader.getProperty(UobConstants.SPARK_EXECUTOR_CORES);
		try {
		SparkExecParamsModel sparkPropModel = new SparkExecParamsModel();
		sparkPropModel = ingestDao.getSparkExecutionProperties(procId, ctryCd, UobConstants.SPARK_INGESTION_PARAM_NM);
		if(sparkPropModel !=null) {
			driverMemoryPropValue = sparkPropModel.getDriverMemory();
			executorMemoryPropValue = sparkPropModel.getExecutorMemory();
			executorInstancePropValue = sparkPropModel.getExecutorInstances();
			executorCoresPropValue = sparkPropModel.getExecutorCores();
		}
		logger.debug("Checked metadata table for Spark Properties.");
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		logger.info("setting dynamic spark properties from Property File Info : "+ driverMemoryPropValue + " - " + executorMemoryPropValue + " - " + executorInstancePropValue+" - "+executorCoresPropValue);
		sparkDynamicProperties.put(SparkLauncher.DRIVER_MEMORY, driverMemoryPropValue);
		sparkDynamicProperties.put(SparkLauncher.EXECUTOR_MEMORY, executorMemoryPropValue);
		sparkDynamicProperties.put("spark.executor.instances", executorInstancePropValue);
		sparkDynamicProperties.put("spark.executor.cores", executorCoresPropValue);
		
		/*
		if (sizeInMB < 1024) {
			sparkDynamicProperties.put(SparkLauncher.DRIVER_MEMORY, "2g");
			sparkDynamicProperties.put(SparkLauncher.EXECUTOR_MEMORY, "4g");
			sparkDynamicProperties.put("spark.executor.instances", "2");
		} else if (sizeInMB > 1024 && sizeInMB < 10240) {
			sparkDynamicProperties.put(SparkLauncher.DRIVER_MEMORY, "4g");
			sparkDynamicProperties.put(SparkLauncher.EXECUTOR_MEMORY, "8g");
			sparkDynamicProperties.put("spark.executor.instances", "4");
		} else if (sizeInMB > 10240 && sizeInMB < 30720) {
			sparkDynamicProperties.put(SparkLauncher.DRIVER_MEMORY, "8g");
			sparkDynamicProperties.put(SparkLauncher.EXECUTOR_MEMORY, "12g");
			sparkDynamicProperties.put("spark.executor.instances", "8");
		} else {
			sparkDynamicProperties.put(SparkLauncher.DRIVER_MEMORY, "12g");
			sparkDynamicProperties.put(SparkLauncher.EXECUTOR_MEMORY, "16g");
			sparkDynamicProperties.put("spark.executor.instances", "12");
		}*/
		return sparkDynamicProperties;
	}

}