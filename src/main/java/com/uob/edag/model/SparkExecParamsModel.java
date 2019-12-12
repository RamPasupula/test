package com.uob.edag.model;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.utils.UobUtils;
import org.apache.log4j.Logger;

import java.text.MessageFormat;

public class SparkExecParamsModel {

	private String procId;
	private String ctryCd;
	private String paramNm;
	private String driverMemory;
	private String executorMemory;
	private String executorInstances;
	private String executorCores;

    protected Logger logger = Logger.getLogger(getClass());

    private static final MessageFormat INSERT_SPARK_EXEC_PARAMS_TEMPLATE = new MessageFormat(
            UobUtils.ltrim("INSERT INTO EDAG_SPARK_EXECUTION_PARAMS(PROC_ID, CTRY_CD, PARAM_NM, DRIVER_MEMORY, EXECUTOR_MEMORY, ") +
                    UobUtils.ltrim(" NO_OF_EXECUTORS, EXECUTOR_CORES) ") +
                    UobUtils.ltrim("VALUES(''{0}'', ''{1}'', ''{2}'', ''{3}'', ''{4}'', ''{5}'', ''{6}''); ")
    );

    private static final MessageFormat REMOVE_SPARK_EXEC_PARAMS_TEMPLATE = new MessageFormat(
            "DELETE FROM EDAG_SPARK_EXECUTION_PARAMS WHERE PROC_ID = ''{0}'' AND CTRY_CD=''{1}'' AND PARAM_NM=''{2}''; "
    );

    public String getProcId() {
		return procId;
	}
	public void setProcId(String procId) {
		this.procId = procId;
	}
	public String getCtryCd() {
		return ctryCd;
	}
	public void setCtryCd(String ctryCd) {
		this.ctryCd = ctryCd;
	}
	public String getParamNm() {
		return paramNm;
	}
	public void setParamNm(String paramNm) {
		this.paramNm = paramNm;
	}
	public String getDriverMemory() {
		return driverMemory;
	}
	public void setDriverMemory(String driverMemory) {
		this.driverMemory = driverMemory;
	}
	public String getExecutorMemory() {
		return executorMemory;
	}
	public void setExecutorMemory(String executorMemory) {
		this.executorMemory = executorMemory;
	}
	public String getExecutorInstances() {
		return executorInstances;
	}
	public void setExecutorInstances(String executorInstances) {
		this.executorInstances = executorInstances;
	}
	public String getExecutorCores() {
		return executorCores;
	}
	public void setExecutorCores(String executorCores) {
		this.executorCores = executorCores;
	}

    public String getInsertProcessMasterSql() {
        String result = INSERT_SPARK_EXEC_PARAMS_TEMPLATE.format(new Object[] {getProcId(), getCtryCd(),
                getParamNm(), getDriverMemory(),
                getExecutorMemory(), getExecutorInstances(),
                getExecutorCores()});
        logger.debug("Insert to Spark Exec Params SQL: " + result);
        return result;
    }

    public String getRemoveProcessMasterSql() {
        String result = REMOVE_SPARK_EXEC_PARAMS_TEMPLATE.format(new Object[] {this.getProcId(), this.getCtryCd(), this.getParamNm()});
        logger.debug("Remove Spark Exec Params statement: " + result);
        return result;
    }
}
