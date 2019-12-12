package com.uob.edag.mappers;

import java.util.List;
import java.util.Map;

import com.uob.edag.model.UNSFileTypeModel;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 12/01/2016
 * @Description : The file is the MyBatis mapper class for the Impala DAO class.
 */

public interface ImpalaMapper extends HiveMapper {

  void runRefresh(Map<String, String> params);
  
  void invalidate(Map<String, String> params);
  
  List<UNSFileTypeModel> getAttachNamesWithDocTypeImp(Map<String, String> params);

}
