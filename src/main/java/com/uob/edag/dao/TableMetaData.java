package com.uob.edag.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

public class TableMetaData {
	
	protected Logger logger = Logger.getLogger(getClass());

	private List<ColumnMetaData> columnMetadata = new ArrayList<ColumnMetaData>(); 
	private String schemaName;
	private String name;
	private String catalogName;
	private Map<String, String> partitionMap = new HashMap<String, String>();
	
	TableMetaData(String tableName) {
		this.name = tableName;
	}
	
	public void setPartition(String partition) {
		this.partitionMap = parsePartition(partition);
	}
	
	public Map<String, String> getPartitionMap() {
		return this.partitionMap;
	}
	
	private Map<String, String> parsePartition(String partition) {
  	Map<String, String> partitionMap = new HashMap<String, String>();

  	for(String partitionKVPair : StringUtils.trimToEmpty(partition).split(",", -1)) {
  		if (StringUtils.isNotBlank(partitionKVPair)) {
	  		String[] kvPair = StringUtils.trimToEmpty(partitionKVPair).split("=", -1);
	  		if (kvPair.length == 2) {
	  			partitionMap.put(kvPair[0], kvPair[1]);
	  		} else {
	  			logger.warn(partitionKVPair + " doesn't seem to be a valid partition key / value pair");
	  		}
  		}
  	}
  	
  	return partitionMap;
  }
	
	public void setSchemaName(String name) {
		this.schemaName = name;
	}
	
	public void setCatalogName(String name) {
		this.catalogName = name;
	}
	
	public String getSchemaName() {
		return this.schemaName;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getCatalogName() {
		return this.catalogName;
	}
	
	public List<ColumnMetaData> getColumnMetadata() {
		return this.columnMetadata;
	}
	
	public String toString() {
		return new ToStringBuilder(this).append("name", name)
				                            .append("schemaName", schemaName)
				                            .append("catalogName", catalogName)
				                            .append("columnCount", columnMetadata.size()).toString();
	}
}
