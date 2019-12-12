package com.uob.edag.utils;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Created by cs186076 on 25/5/17.
 */
public class StringUtils {
	
	  private static Logger logger = Logger.getLogger(StringUtils.class);
	
    /***
     * Method to replace all the Key variables with the values from the Map.
     *
     * @param map
     * @param inputString
     * @return
     */
    public static String replaceAll(Map<String,String> map, String inputString){
        Iterator<String> keyIterator = map.keySet().iterator();
        String strInput = inputString;
        while(keyIterator.hasNext()){
            String s = keyIterator.next();
            strInput.replaceAll(s,map.get(s));
        }
        return strInput;
    }
    
    public static String normalizeForHive(String input) {
    	return normalizeForHive(input, false);
    }
    
    public static String normalizeForHive(String input, boolean allowSpace) {
    	String result = null;
    	
    	if (input != null) {
    		if (allowSpace) {
    			result = input.replaceAll("[^a-zA-z_0-9 ]|\\^|\\`|\\\\|\\[|\\]", "_");
    		} else {
    	    result = input.replaceAll("[^a-zA-z_0-9]|\\^|\\`|\\\\|\\[|\\]", "_");
    		}  
    	  String tempResult;
    	  do {
    	  	tempResult = result;
    	  	result = result.replaceAll("__", "_");
    	  	
    	  } while (!tempResult.equals(result));
    	  result = result.replaceFirst("^_+", "");
  	    result = result.replaceFirst("_+$", "");
    	}
    	
    	logger.debug(input + " normalized for Hive to " + result);
    	return result;		
    }
}
