package com.uob.edag.connection;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGValidationException;

public abstract class MyBatisConnectionFactory {

    protected Logger logger = Logger.getLogger(getClass());

    private SqlSessionFactory sqlMapper;

    protected abstract Properties getConnectionProperties() throws EDAGIOException;

    protected abstract String getResourceName();

    protected void init(String resourceName, Properties connectionProperties) throws EDAGIOException, EDAGMyBatisException {
        try (Reader resourceReader = Resources.getResourceAsReader(resourceName)) {
            try {
                sqlMapper = new SqlSessionFactoryBuilder().build(resourceReader, connectionProperties);
                logger.debug("SQL session factory built based on " + resourceName);
            } catch (PersistenceException e) {
                throw new EDAGMyBatisException(EDAGMyBatisException.CANNOT_BUILD_SESSION_FACTORY, resourceName, e.getMessage());
            }
        } catch (IOException e1) {
            throw new EDAGIOException(EDAGIOException.CANNOT_READ_RESOURCE, resourceName, e1.getMessage());
        }
    }

  /**
   * This method is used to get the session object from mybatis
   * @return the SQLSessionFactory object
   * @throws EDAGMyBatisException 
   * @throws EDAGValidationException 
   * @throws EDAGIOException 
   * @throws Exception when there is error creating the connection.
   */
  public SqlSessionFactory getSession() throws EDAGMyBatisException {
    if (sqlMapper == null) {
    	try {
				init(getResourceName(), getConnectionProperties());
			} catch (EDAGIOException e) {
				throw new EDAGMyBatisException(EDAGMyBatisException.CANNOT_INITIALIZE_SESSION_FACTORY, e.getMessage());
			}
    }
    
    return sqlMapper;
  }
}
