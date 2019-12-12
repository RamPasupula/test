package com.uob.edag.mappers;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Param;

import com.uob.edag.model.AttMasterFileModel;
import com.uob.edag.model.CountryAttributes;
import com.uob.edag.model.DestModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessInstanceModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.model.ProcessParam;
import com.uob.edag.model.SolrCollectionModel;
import com.uob.edag.model.SourceTableDetail;
import com.uob.edag.model.SparkExecParamsModel;
import com.uob.edag.model.StageModel;
import com.uob.edag.model.TDUnsupportedCharacterReplacement;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The class is the MyBatis mapper class for the Process
 *              Ingestion class.
 */
public interface IngestionMapper {
	
  ProcessModel retrieveProcessMaster(String procId);

  List<ProcessModel> retrieveProcessList();

  List<CountryAttributes> retrieveProcessCountry(String procId);

  DestModel retrieveLoadProcess(String procId);
  
  HadoopModel retrieveLoadProcessByTargetTable(Map<String, String> params);

  String retrieveProcessFileName(String procInstanceId);
  
  SourceTableDetail retrieveSourceTableDetail(String procId);

  FileModel retrieveFileDetails(String procId);

  List<FieldModel> retrieveFieldDetails(int fileId);

  List<FieldModel> retrieveControlFieldDetails(int fileId);

  List<Integer> retrieveFieldStdRules(FieldModel fldModel);

  List<ProcessParam> retrieveLoadParams(String procId);

  void insertProcessLog(ProcessInstanceModel procInstance);

  void updateProcessLog(ProcessInstanceModel procInstance);
  
  void updateProcessLogFileName(ProcessInstanceModel procInstance);

  void insertStageLog(StageModel stgModel);

  void updateStageLog(StageModel stgModel);
  
  void updateStageNewStatus(StageModel stgModel);

  String evaluateBizDtExpr(String expression);

  ProcessInstanceModel getPrevRunStatus(ProcessInstanceModel procInsModel);
  
  ProcessInstanceModel getProcessInstance(String procInstanceId);

  List<StageModel> getStageInfo(ProcessInstanceModel procInsModel);
  
  List<StageModel> getStageInfoByProcessAndStageID(Map<String, Object> params);
  
  String getBizDate(@Param("countryCd") String countryCd, @Param("procId") String procId);
  
  List<TDUnsupportedCharacterReplacement> getTDUnsupportedCharacterReplacement(@Param("procId") String procId, @Param("ctryCd") String ctryCd);
  
  List<Map<String, String>> getControlCharsReplacement(@Param("procId") String procId, @Param("ctryCd") String ctryCd);
  
  void addTDUnsupportedCharacterReplacement(TDUnsupportedCharacterReplacement toAdd);
  
  void deleteTDUnsupportedCharacterReplacement(TDUnsupportedCharacterReplacement toDelete);
  
  List<Map<String, String>> getFieldNamePatterns(@Param("procId") String procId);

  void insertStageLogForAttachments(StageModel stgModel);
  
  void insertLoadedFileLog(ProcessInstanceModel procInstance);
  
  String retrieveAdobeProcSubProcMap(String procId);
  
  String retrieveSourceDirectory(String procId);
  
  String getAdobeProcStatus(List<String> procIdList);
  
  void updateFinalProcessLog(@Param("status") String status, @Param("errorTxt") String errorTxt, @Param("procIdList") List<String> procIdList);

  List<String> getOrderedFieldNames(String procId);
  
    List<SolrCollectionModel> retrieveSolrCollection();
  
  List<AttMasterFileModel> retrieveT11HiveQL(String processId);
  
  String retrievePreProcessFlag(@Param("countryCd") String countryCd, @Param("procId") String procId);
  
  String retrievePreProcessClassName(String procId);

  void updateProcessLogFileSizeTime(ProcessInstanceModel procInstance);
  
  String getPrevRunProcInstanceId(ProcessInstanceModel procInsModel);
  
  ProcessInstanceModel getProcInstanceModelForIndexing(@Param("bizDate") String bizDate, @Param("procId") String procId,@Param("sourceSystemId") String sourceSystemId, @Param("countryCd") String countryCd,@Param("hourToRun") String hourToRun);

  SparkExecParamsModel getSparkExecutionProperties(@Param("procId") String procId, @Param("ctryCd") String ctryCd, @Param("paramNm") String paramNm);

}
