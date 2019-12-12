package com.uob.edag.mappers;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface TeradataMapper {

	Integer getRowCount(Map<String, String> params);
	
	BigDecimal getSumOfHashSum(Map<String, String> params);
	
	void dropTable(Map<String, String> params);
	
	void truncateTable(Map<String, String> params);
	
	List<String> showTable(Map<String, String> params);
}
