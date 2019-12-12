package com.uob.edag.connection;

import java.util.Properties;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGSecurityException;
import com.uob.edag.security.EncryptionUtil;
import com.uob.edag.utils.PropertyLoader;

public class TeradataConnectionFactory extends DBMSConnectionFactory {
	
	public static final String ENCRYPTED_PASSWORD_PREFIX = "ENCRYPTED_PASSWORD(";
	public static final String TD_WALLET_PREFIX = "$tdwallet(";
  private static TeradataConnectionFactory factory;
	
  public static TeradataConnectionFactory getFactory() {
  	if (factory == null) {
  		factory = new TeradataConnectionFactory();
  	}
  	
  	return factory;
  }
	
	protected Properties getConnectionProperties() throws EDAGIOException {
		Properties properties = new Properties();

		String jdbcUrl = PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_URL);
		
		String jdbcUrlParam = "/";
		for (String urlParam : PropertyLoader.getAllPropertyKeys(UobConstants.TERADATA_JDBC_URL_PARAM_PREFIX)) {
			String key = urlParam.substring(UobConstants.TERADATA_JDBC_URL_PARAM_PREFIX.length());
			String value = PropertyLoader.getProperty(urlParam);
			if (value.indexOf(",") >= 0) {
				value = "'" + value + "'";
			}
			
			jdbcUrlParam += "/".equals(jdbcUrlParam) ? key + "=" + value : "," + key + "=" + value;
		}
		
		if (!"/".equals(jdbcUrlParam)) {
			jdbcUrl += jdbcUrlParam;
		}
		
		logger.debug("Teradata JDBC URL: " + jdbcUrl);
		properties.setProperty("JDBC.Driver", PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_DRIVER));
    properties.setProperty("JDBC.ConnectionURL", jdbcUrl);
    properties.setProperty("JDBC.Username", PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_USER));
    
    String password = PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_PASSWORD);
    if (!password.startsWith(ENCRYPTED_PASSWORD_PREFIX) && !password.contains(TD_WALLET_PREFIX)) {
    	try {
				password = new EncryptionUtil().decrypt(password);
			} catch (EDAGSecurityException e) {
				throw new EDAGIOException(EDAGIOException.CANNOT_GET_PROPERTY, "JDBC.Password", e.getMessage());
			}
    }
    properties.setProperty("JDBC.Password", password);
    
    properties.setProperty("JDBC.PoolPingEnabled", PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_POOL_PING_ENABLED));
    properties.setProperty("JDBC.PoolPingQuery", PropertyLoader.getProperty(UobConstants.TERADATA_JDBC_POOL_PING_QUERY));
		
		return properties;
	}

	protected String getResourceName() {
		logger.debug("TeradataConnectionFactory uses td-db-configuration.xml");
		return "td-db-configuration.xml";
	}
}
