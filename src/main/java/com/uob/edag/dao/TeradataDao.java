package com.uob.edag.dao;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.TeradataConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.TeradataMapper;

public class TeradataDao extends AbstractDao {
	
	public static final PredefinedException CANNOT_GET_ROW_COUNT = new PredefinedException(TeradataDao.class, "CANNOT_GET_ROW_COUNT", "Unable to get row count of table {0}: {1}");
	public static final PredefinedException CANNOT_GET_HASH_SUM = new PredefinedException(TeradataDao.class, "CANNOT_GET_HASH_SUM", "Unable to get sum of {0}.{1}: {2}");
	public static final PredefinedException CANNOT_DROP_TABLE = new PredefinedException(TeradataDao.class, "CANNOT_DROP_TABLE", "Unable to drop table {0}: {1}");
	public static final PredefinedException CANNOT_TRUNCATE_TABLE = new PredefinedException(TeradataDao.class, "CANNOT_TRUNCATE_TABLE", "Unable to truncate table {0}: {1}");
	public static final PredefinedException CANNOT_SHOW_TABLE = new PredefinedException(TeradataDao.class, "CANNOT_SHOW_TABLE", "Unable to extract {0}.{1} DDL: {2}");

	public int getRowCount(String tableName, boolean tableMustExist) throws EDAGMyBatisException {
    try (SqlSession session = openSession(true)) {
    	Map<String, String> params = new HashMap<String, String>();
    	params.put("tableName", tableName);
    	Integer result = session.getMapper(TeradataMapper.class).getRowCount(params);
      logger.debug(tableName + " has " + result + " rows");
      return result == null ? 0 : result;
    } catch (PersistenceException e) {
    	String err = e.getMessage();
    	if (tableMustExist || !(err.contains("Object") && err.contains("does not exist"))) {
    		throw new EDAGMyBatisException(TeradataDao.CANNOT_GET_ROW_COUNT, tableName, err);
    	} else {
    		logger.debug(tableName + " doesn't exist, returning 0 as row count");
    		return 0;
    	}
    }
	}
	
	public int getRowCount(String tableName) throws EDAGMyBatisException {
		return getRowCount(tableName, true);
	}
	
	public String getSumOfHashSumAsString(String tableName, String hashSumColumn, int precision) throws EDAGMyBatisException {
		if (precision < 0) {
			throw new EDAGMyBatisException("Precision cannot be less than 0");
		}
		
		MathContext ctx = new MathContext(precision);
		BigDecimal result = getSumOfHashSum(tableName, hashSumColumn).round(ctx);
		logger.debug("Formatted sum of hash sum: " + result.toPlainString());
		return result.toPlainString();
	}
	
	public BigDecimal getSumOfHashSum(String tableName, String hashSumColumn) throws EDAGMyBatisException {
		Map<String, String> params = new HashMap<String, String>();
		params.put("tableName", tableName);
		params.put("columnName", hashSumColumn);
		
		try (SqlSession session = openSession(true)) {
			BigDecimal result = session.getMapper(TeradataMapper.class).getSumOfHashSum(params);
			logger.debug("Sum of " + tableName + "." + hashSumColumn + ": " + result);
			return result == null ? new BigDecimal(0) : result;
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(TeradataDao.CANNOT_GET_HASH_SUM, tableName, hashSumColumn, e.getMessage());
		}
	}
	
	public void dropTable(String tableName) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			Map<String, String> params = new HashMap<String, String>();
    	params.put("tableName", tableName);
			session.getMapper(TeradataMapper.class).dropTable(params);
			logger.debug(tableName + " dropped");
		} catch (PersistenceException e) {
			String err = e.getMessage();
			if (err.contains("Object") && err.contains("does not exist")) {
				logger.debug("Unable to drop " + tableName + ": " + err);
			} else {
				throw new EDAGMyBatisException(TeradataDao.CANNOT_DROP_TABLE, tableName, err);
			}
		} 
	}
	
	public String showTable(String databaseName, String tableName) throws EDAGMyBatisException {
		return showTable(databaseName + "." + tableName);
	}
	
	public String showTable(String fullTableName) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			String result = "";
			Connection conn = session.getConnection();
			boolean closingStatement = false;
			String statement = "show table " + fullTableName;
			
			try (PreparedStatement ps = conn.prepareStatement(statement)) {
				ResultSet rs = ps.executeQuery();
				while (rs.next()) {
					result += rs.getString(1);
				}
				
				closingStatement = true;
			} catch (SQLException e) {
				if (closingStatement) {
					logger.warn("Unable to close statement: " + e.getMessage());
				} else {
					throw new EDAGMyBatisException(EDAGMyBatisException.CANNOT_EXECUTE_SQL, statement, "<no params", e.getMessage());
				}
			}
			
			logger.debug("Create table statement for " + fullTableName + ": " + result);
			return result;
		}
	}
	
	public boolean truncateTable(String tableName) throws EDAGMyBatisException {
		try (SqlSession session = openSession(true)) {
			Map<String, String> params = new HashMap<String, String>();
    	params.put("tableName", tableName);
			session.getMapper(TeradataMapper.class).truncateTable(params);
			logger.info(tableName + " truncated");
			return true;
		} catch (PersistenceException e) {
			String err = e.getMessage();
			if (err.contains("Object") && err.contains("does not exist")) {
				logger.info(tableName + " is not dropped since it doesn't exist");
				return false;
			} else {
				throw new EDAGMyBatisException(TeradataDao.CANNOT_TRUNCATE_TABLE, tableName, err);
			}	
		} 
	}

	public TeradataDao() {
		super(TeradataConnectionFactory.getFactory());
	}
}
