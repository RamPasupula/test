package com.uob.edag.mappers;

import java.util.List;

import com.uob.edag.model.DestModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.StageModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The class is the MyBatis mapper class for the Process
 *              ExportDao class.
 */
public interface ExportMapper {
	
  ProcessModel retrieveProcessMaster(String procId);

  List<CountryAttributes> retrieveProcessCountry(String procId);

  DestModel retrieveExportProcess(String procId);

  String retrieveProcessFileName(String procInstanceId);

  List<ProcessParam> retrieveLoadParams(String procId);

  void insertProcessLog(ProcessInstanceModel procInstance);

  void updateProcessLog(ProcessInstanceModel procInstance);
  
  void updateProcessLogFileName(ProcessInstanceModel procInstance);

  void insertStageLog(StageModel stgModel);

  void updateStageLog(StageModel stgModel);

  String evaluateBizDtExpr(String expression);

  ProcessInstanceModel getPrevRunStatus(ProcessInstanceModel procInsModel);

  List<StageModel> getStageInfo(ProcessInstanceModel procInsModel);
}
