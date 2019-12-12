package com.uob.edag.processor;

import com.uob.edag.exception.EDAGException;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;

public interface IngestionProcessor {

	void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel procModel, String bizDate, String ctryCd,
			boolean forceRerun, String forceFileName) throws EDAGException;

}