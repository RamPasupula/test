/**
* @Author       : Daya Venkatesan
* @Date of Creation: 10/24/2016
* @Description    : The file is used for initializing mybatis connections to the metadata database
* 
*/

package com.uob.edag.connection;

import java.util.Properties;

import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGSecurityException;
import com.uob.edag.security.EncryptionUtil;
import com.uob.edag.utils.PropertyLoader;

public class DBMSConnectionFactory extends MyBatisConnectionFactory {
	
	private static DBMSConnectionFactory factory;
	
	private EncryptionUtil encUtil = new EncryptionUtil();
	
  public static DBMSConnectionFactory getFactory() {
  	if (factory == null) {
  		factory = new DBMSConnectionFactory();
  	}
  	
  	return factory;
  }
  
	@Override
	protected Properties getConnectionProperties() throws EDAGIOException {
		Properties properties = new Properties();
    properties.setProperty("JDBC.Driver", PropertyLoader.getProperty("JDBC.Driver"));
    properties.setProperty("JDBC.ConnectionURL", PropertyLoader.getProperty("JDBC.ConnectionURL"));
    properties.setProperty("JDBC.Username", PropertyLoader.getProperty("JDBC.Username"));
    try {
      properties.setProperty("JDBC.Password", encUtil.decrypt(PropertyLoader.getProperty("JDBC.Password")));
    } catch (EDAGSecurityException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_GET_PROPERTY, "JDBC.Password", e.getMessage());
		}
    
    return properties;
	}

	@Override
	protected String getResourceName() {
		logger.debug("DBMSConnectionFactory uses db-configuration.xml");
		return "db-configuration.xml";
	}
}  
