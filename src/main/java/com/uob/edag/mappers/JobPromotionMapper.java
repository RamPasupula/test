package com.uob.edag.mappers;

import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessModel;

import java.util.List;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The file is the MyBatis mapper class for the Job Promotion DAO
 *              class.
 */
public interface JobPromotionMapper {
	
  ProcessModel retrieveProcessMaster(int procId);

  HadoopModel retrieveLoadProcess(int procId);

  FileModel retrieveFileDetails(int procId);

  List<FieldModel> retrieveFieldDetails(int fileId);

  List<FieldModel> retrieveControlFieldDetails(int fileId);
}
