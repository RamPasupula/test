package com.uob.edag.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;

import com.google.common.base.Charsets;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;

/**
 * @Author : Daya Venkatesan
 * @Author: ChandraSekhar Saripaka
 * @Date of Creation: 11/01/2016
 * @Date of modification: 25/05/2017
 * @Description : The class is used to load the framework configuration property
 *              files into memory for every processing.
 * 
 */

public class PropertyLoader {
	
	private static final Logger logger = Logger.getLogger(PropertyLoader.class);
	private static VelocityContext ctx;
	private static Properties props = null;
	
	private static Map<String, String> charsetMap = new HashMap<String, String>();
	private static Map<String, String> encodingMap = new HashMap<String, String>();

	/**
	 * This method will return the list of properties. If the properties is
	 * null, it will load the properties from the config file.
	 * Added the DateFormat Conf file to load the date format properties.
	 * 
	 * @return the Properties object containing the list of properties
	 * @throws EDAGIOException 
	 */
	private static void init() throws EDAGIOException {
		if (props == null) {
			props = new Properties();
			loadProperties(System.getProperty(UobConstants.FRAMEWORK_CONF_FILE));
			loadProperties(System.getProperty(UobConstants.DATABASE_CONF_FILE));
			
			ctx = new VelocityContext();
			for (String key : props.stringPropertyNames()) {
				Object result = props.get(key);
				ctx.put(key, result);
			}
		}
	}
	
	public static String getProperty(String key) {
		return getProperty(key, false, null);
	}
	
	public static String getProperty(String key, boolean resolveVariables) {
		return getProperty(key, resolveVariables, null);
	}
	
	public static String getProperty(String key, Map<String, Object> contextParams) {
		return getProperty(key, true, contextParams);
	}

	private static String getProperty(String key, boolean resolveVariables, Map<String, Object> contextParams) {
		if (props == null) {
			try {
				init();
			} catch (EDAGIOException e) {
				// we throw runtime exception since getProperty() is called from all over the place, don't want to change the signature of this method
				throw new RuntimeException(e);
			}
		}
		
		String result = props.getProperty(key);
		if (resolveVariables && result != null) {
		  StringWriter out = new StringWriter();
			VelocityUtils.evaluate(new VelocityContext(contextParams, ctx), out, key, result);
			logger.debug(result + " evaluated to " + out);
			result = out.toString();
		}
		
		return result;
	}

	/**
	 * This method will load a configuration file into Properties object.
	 * 
	 * @param fileName
	 *            The config file name from which the properties is to be loaded
	 * @throws UobIOException 
	 */
	private static void loadProperties(String fileName) throws EDAGIOException {
		try (InputStreamReader isr = new InputStreamReader(new FileInputStream(fileName), Charsets.UTF_8)) {
			props.load(isr);
			logger.info("Properties loaded from " + fileName);
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_LOAD_PROPERTIES_FROM_FILE, fileName, e.getMessage());
		} 
	}
	
	public static Set<String> getAllPropertyKeys() throws EDAGIOException {
		if (props == null) {
			init();
		}
		
		return props.stringPropertyNames();
	}
	
	public static Set<String> getAllPropertyKeys(String prefix) throws EDAGIOException {
		Set<String> result = new TreeSet<String>();
		
		for (String propertyKey : getAllPropertyKeys()) {
			if (propertyKey.startsWith(prefix)) {
				result.add(propertyKey);
			}
		}
		
		return result;
	}
	
	public static Set<String> getAllPropertyKeys(String prefix, String suffix) throws EDAGIOException {
		Set<String> result = new TreeSet<String>();
		
		for (String propertyKey : getAllPropertyKeys()) {
			if ((prefix == null || propertyKey.startsWith(prefix)) &&
					(suffix == null || propertyKey.endsWith(suffix))) {
				result.add(propertyKey);
			}
		}
		
		return result;
	}
	
	private static String getCharsetOrEncoding(String countryCode, Map<String, String> map, String suffix, String defaultKey) {
		String result = null;
		
		if (map.containsKey(countryCode)) {
			result = map.get(countryCode);
		} else {
			result = PropertyLoader.getProperty(countryCode + suffix);
			if (StringUtils.isBlank(result)) {
				result = PropertyLoader.getProperty(defaultKey);
			}
			
			map.put(countryCode, result);
			logger.info((UobConstants.DEFAULT_CHARSET.equals(defaultKey) ? "Charset" : "Encoding") +
					        " for country " + countryCode + " set to " + result);
		}
		
		return result;
	}
	
	public static String getEncoding(String countryCode) {
		return getCharsetOrEncoding(countryCode, encodingMap, "_ENCODING", UobConstants.DEFAULT_ENCODING);
	}
	
	public static String getCharsetName(String countryCode) {
		return getCharsetOrEncoding(countryCode, charsetMap, "_CHARSET", UobConstants.DEFAULT_CHARSET);
	}
}
