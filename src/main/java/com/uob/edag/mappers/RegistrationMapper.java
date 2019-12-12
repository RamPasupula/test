package com.uob.edag.mappers;

import java.util.List;

import com.uob.edag.model.RuleModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/27/2016
 * @Description : The file is the MyBatis mapper class for the Registration DAO
 *              class.
 */

public interface RegistrationMapper {
	
  int checkFileExists(String procId);

  String getProcId(String fileName);

  List<RuleModel> retrieveStdRules();

  int selectControlFileId();

  int selectFileId();

  int selectAlertId();
}
