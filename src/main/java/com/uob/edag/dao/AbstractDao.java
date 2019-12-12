package com.uob.edag.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.apache.log4j.Logger;

import com.uob.edag.connection.MyBatisConnectionFactory;
import com.uob.edag.exception.EDAGMyBatisException;

public abstract class AbstractDao {

	protected Logger logger = Logger.getLogger(getClass());
	protected MyBatisConnectionFactory factory;
	
	protected AbstractDao(MyBatisConnectionFactory factory) {
		this.factory = factory;
	}
	
	protected SqlSession openSession() throws EDAGMyBatisException {
		logger.debug("Getting a session from " + factory.getClass().getName());
		return factory.getSession().openSession();
	}
	
	protected SqlSession openSession(boolean autoCommit) throws EDAGMyBatisException {
		logger.debug("Getting a session from " + factory.getClass().getName() + " autocommit is " + autoCommit);
		return factory.getSession().openSession(autoCommit);
	}
	
	public TableMetaData getTableMetadata(String tableName) throws EDAGMyBatisException {
		TableMetaData tableMeta = null;
		String statement = getMetadataSelectStatement(tableName);
		
		try (SqlSession session = openSession(true)) {
			Connection conn = session.getConnection();
			boolean closingStatement = false;
			try (PreparedStatement ps = conn.prepareStatement(statement)) {
				try (ResultSet rs = ps.executeQuery()) {
					logger.debug("Getting result set metadata");
					ResultSetMetaData rsMeta = rs.getMetaData();
					
					boolean isTypeSupported = true;
					boolean isLabelSupported = true;
					boolean isDisplaySizeSupported = true;
					boolean isPrecisionSupported = true;
					boolean isScaleSupported = true;
					boolean isTypeNameSupported = true;
					boolean isClassNameSupported = true;
					logger.debug("Iterating metadata columns");
					for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
						if (i == 1) {
							tableMeta = new TableMetaData(rsMeta.getTableName(i));
							logger.debug("Table name is " + tableMeta.getName());
							tableMeta.setSchemaName(rsMeta.getSchemaName(i));
							logger.debug("Schema name is " + tableMeta.getSchemaName());
							
							try {
								tableMeta.setCatalogName(rsMeta.getCatalogName(i));
							} catch (SQLException e) {
								logger.debug("Unable to get catalog name: " + e.getMessage());
							}
							
							ColumnMetaData colMeta = new ColumnMetaData(rsMeta.getColumnName(i));
							logger.debug("Column " + i + " name is " + colMeta.getName());
							
							try {
						  	colMeta.setLabel(rsMeta.getColumnLabel(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column label: " + e.getMessage());
						  	isLabelSupported = false;
						  }
							
							try {
								colMeta.setDisplaySize(rsMeta.getColumnDisplaySize(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column display size: " + e.getMessage());
						  	isDisplaySizeSupported = false;
						  }
							
							try {
								colMeta.setType(rsMeta.getColumnType(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column type: " + e.getMessage());
						  	isTypeSupported = false;
						  }
							
							try {
								colMeta.setPrecision(rsMeta.getPrecision(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column precision: " + e.getMessage());
						  	isPrecisionSupported = false;
						  }
							
							try {
								colMeta.setScale(rsMeta.getScale(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column scale: " + e.getMessage());
						  	isScaleSupported = false;
						  }
							
							try {
								colMeta.setTypeName(rsMeta.getColumnTypeName(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column type name: " + e.getMessage());
						  	isTypeNameSupported = false;
						  }
							
							try {
								colMeta.setClassName(rsMeta.getColumnClassName(i));
						  } catch (SQLException e) {
						  	logger.debug("Unablet to get column class name: " + e.getMessage());
						  	isClassNameSupported = false;
						  }
							
							tableMeta.getColumnMetadata().add(colMeta);
						} else {
							ColumnMetaData colMeta = new ColumnMetaData(rsMeta.getColumnName(i));
							colMeta.setLabel(isLabelSupported ? rsMeta.getColumnLabel(i) : null);
							colMeta.setDisplaySize(isDisplaySizeSupported ? rsMeta.getColumnDisplaySize(i) : -1);
							colMeta.setType(isTypeSupported ? rsMeta.getColumnType(i) : -1);
							colMeta.setPrecision(isPrecisionSupported ? rsMeta.getPrecision(i) : -1);
							colMeta.setScale(isScaleSupported ? rsMeta.getScale(i) : -1);
							colMeta.setTypeName(isTypeNameSupported ? rsMeta.getColumnTypeName(i) : null);
							colMeta.setClassName(isClassNameSupported ? rsMeta.getColumnClassName(i) : null);
							tableMeta.getColumnMetadata().add(colMeta);
						}
					}
				}
			} catch (SQLException e) {
				if (closingStatement) {
					logger.warn("Unable to close preparedStatement " + statement + ": " + e.getMessage());
				} else {
					throw new PersistenceException(e);
				}
			}
		}
		
		logger.debug("Metadata of table " + tableName + ": " + tableMeta);
		return tableMeta;
	}
	
	protected String getMetadataSelectStatement(String tableName) {
		return "select * " +
				   "from " + tableName + " " +
				   "limit 1 "; // subclass should override this method whenever necessary
	}

	public int executeUpdate(String statement, Object[] params) throws EDAGMyBatisException {
		Integer result = -1;
		try (SqlSession session = openSession(true)) {
			Connection conn = session.getConnection();
			boolean closingStatement = false;
			try (PreparedStatement ps = conn.prepareStatement(statement)) {
				if (params != null) {
					for (int i = 0; i < params.length; i++) {
						ps.setObject(i + 1, params[i]);
					}
				}
				
				result = ps.executeUpdate();
				logger.debug(statement + " with parameters " + arrayToList(params) + " affects " + result + " row(s)");
				closingStatement = true;
			} catch (SQLException e) {
				if (closingStatement) {
					logger.warn("Unable to close preparedStatement " + statement + ": " + e.getMessage());
				} else {
					throw new PersistenceException(e);
				}
			}
		} catch (PersistenceException e) {
			throw new EDAGMyBatisException(EDAGMyBatisException.CANNOT_EXECUTE_SQL, statement, arrayToList(params), e.getMessage());
		}
		
		return result;
	}
	
	private List<Object> arrayToList(Object[] arr) {
		List<Object> result = new ArrayList<Object>();
		if (arr != null) {
			CollectionUtils.addAll(result, arr);
		}
		
		return result;
	}
}
