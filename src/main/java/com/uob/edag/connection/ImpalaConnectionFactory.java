package com.uob.edag.connection;

import java.util.Properties;

import com.uob.edag.utils.PropertyLoader;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 11/01/2017
 * @Description : The file is used for initializing mybatis connections to the
 *              Impala database
 * 
 */

public class ImpalaConnectionFactory extends KerberizedMyBatisConnectionFactory {
	
	private static ImpalaConnectionFactory factory = null;
	
	public static ImpalaConnectionFactory getFactory() {
		if (factory == null) {
			factory = new ImpalaConnectionFactory();
		}
		
		return factory;
	}
  
	@Override
	protected Properties getConnectionProperties() {
		Properties connProps = new Properties();
		
    connProps.setProperty("Impala.JDBC.Driver", PropertyLoader.getProperty("Impala.JDBC.Driver"));
    connProps.setProperty("Impala.JDBC.ConnectionURL", PropertyLoader.getProperty("Impala.JDBC.ConnectionURL"));
    
    return connProps;
	}

	@Override
	protected String getResourceName() {
		logger.debug("ImpalaConnectionFactory uses impala-configuration.xml");
		return "impala-configuration.xml";
	}
}
