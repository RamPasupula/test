package com.uob.edag.model;

import java.io.Serializable;
import java.text.MessageFormat;

import com.uob.edag.utils.UobUtils;

public class LobStampingInfo implements Serializable {

	private static final long serialVersionUID = 2381340367129068950L;

	private static final MessageFormat INSERT_LOB_STAMPING_INFO_SQL = new MessageFormat(UobUtils.ltrim(
			"INSERT INTO EDAG_LOB_STAMPING_SQL_INFO(PROC_ID, LOB_STAMPING_SQL, SITE_ID_COMPUTE_SQL, DEPENDANT_TABLES, HIVE_TABLE_NAME) ")
			+ UobUtils.ltrim("VALUES(''{0}'', {1}, {2}, ''{3}'', ''{4}'');"));

	private static final MessageFormat DELETE_LOB_STAMPING_SQL = new MessageFormat(
			UobUtils.ltrim("DELETE FROM EDAG_LOB_STAMPING_SQL_INFO WHERE PROC_ID = ''{0}'';"));

//	Insert into table (clob_column) values ( to_clob( 'chunk 1' ) || to_clob( 'chunk 2' ) );

	private String processId;

	private String lobStampingSql;

	private String dependentTables;

	private String siteIdComputeSql;

	private String hiveTableName;

	public LobStampingInfo() {

	}

	public LobStampingInfo(String processId, String dependentTables, String lobStampingSql, String siteIdComputeSql,
			String hiveTableName) {
		super();
		this.processId = processId;
		this.lobStampingSql = lobStampingSql;
		this.dependentTables = dependentTables;
		this.siteIdComputeSql = siteIdComputeSql;
		this.hiveTableName = hiveTableName;
	}

	public String getProcessId() {
		return processId;
	}

	public void setProcessId(String processId) {
		this.processId = processId;
	}

	public String getLobStampingSql() {
		return lobStampingSql;
	}

	public void setLobStampingSql(String lobStampingSql) {
		this.lobStampingSql = lobStampingSql;
	}

	public String getDependentTables() {
		return dependentTables;
	}

	public void setDependentTables(String dependentTables) {
		this.dependentTables = dependentTables;
	}

	public String getSiteIdComputeSql() {
		return siteIdComputeSql;
	}

	public void setSiteIdComputeSql(String siteIdComputeSql) {
		this.siteIdComputeSql = siteIdComputeSql;
	}

	public String getHiveTableName() {
		return hiveTableName;
	}

	public void setHiveTableName(String hiveTableName) {
		this.hiveTableName = hiveTableName;
	}

	public String getInsertSQL() {

		String result = INSERT_LOB_STAMPING_INFO_SQL
				.format(new Object[] { this.processId, getEquivalentClobSQL(this.lobStampingSql.replaceAll("'", "''")),
						getEquivalentClobSQL(this.siteIdComputeSql.replaceAll("'", "''")), this.dependentTables, this.hiveTableName });
		return result;
	}

	public String getDeleteSQL() {
		String result = DELETE_LOB_STAMPING_SQL.format(new Object[] { this.processId });
		return result;
	}

	public String getHiveAlterTableSQL() {
		String result = String.format(
				"ALTER TABLE %s ADD COLUMNS (derived_cif STRING COMMENT 'Derived CIF', actual_site_id STRING COMMENT 'Computed SITE ID');",
				this.hiveTableName);
		return result;
	}

	private String getEquivalentClobSQL(String sql) {
		StringBuffer sb = new StringBuffer();
		int noOfChars = 3000;
		if (sql.length() > noOfChars) {
			int addedLength = 0;
			while (addedLength + noOfChars < sql.length()) {
				sb.append("to_clob( '").append(sql.substring(addedLength, addedLength + noOfChars)).append("') || ");
				addedLength += noOfChars;
			}
			sb.append("to_clob( '").append(sql.substring(addedLength)).append("')");
		} else {
			sb.append("'").append(sql).append("'");
		}
		return sb.toString();
	}

}