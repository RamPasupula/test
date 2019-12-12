package com.uob.edag.connection;

import java.util.Properties;

import com.uob.edag.utils.PropertyLoader;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for initializing mybatis connections to the
 *              Hive database
 * 
 */

public class HiveConnectionFactory extends KerberizedMyBatisConnectionFactory {
	
	private static HiveConnectionFactory factory = null;
	
	public static HiveConnectionFactory getFactory() {
		if (factory == null) {
			factory = new HiveConnectionFactory();
		}
		
		return factory;
	}
	
	@Override
	protected Properties getConnectionProperties() {
		Properties connProps = new Properties();
		
    connProps.setProperty("Hive.JDBC.Driver", PropertyLoader.getProperty("Hive.JDBC.Driver"));
    connProps.setProperty("Hive.JDBC.ConnectionURL", PropertyLoader.getProperty("Hive.JDBC.ConnectionURL"));
    
    return connProps;
	}

	@Override
	protected String getResourceName() {
		logger.debug("HiveConnectionFactory uses hive-configuration.xml");
		return "hive-configuration.xml";
	}
}
