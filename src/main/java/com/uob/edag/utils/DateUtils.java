package com.uob.edag.utils;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGValidationException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by cs186076 on 25/5/17.
 */
public class DateUtils {
	
	  public static final String USE_EXTERNALIZED_FORMAT = "USE_EXTERNALIZED_FORMAT";
	
		public static final Map<String, String> VALID_DATE_FORMATS = new HashMap<String, String>();
		private static final String[][] validDateFormats = new String[][] {{"YYYYMMDD", "yyyyMMdd"}, 
			                                                                 {"YYYYMMDDHHMMSS", "yyyyMMddHHmmSS"}, 
			                                                                 {"YYYY-MM-DD", "yyyy-MM-dd"},
			                                                                 {"YYYYMMDDHHMI", "yyyyMMddHHmm"},
			                                                                 {"YYYY-MM-DD HH:MI:SS", "yyyyMMdd HH:mm:ss"},
			                                                                 {"CCYY-MM-DD", "yyyy-MM-dd"},
			                                                                 {"24HHMISS", "HHmmss"},
			                                                                 {"DDMMYY", "ddMMyy"},
			                                                                 {"HHMMSS", "HHmmss"},
			                                                                 {"MON YYYY", "MMM yyyy"},
			                                                                 {"MMM YYYY", "MMM yyyy"},
			                                                                 {"YYYY", "yyyy"},
			                                                                 {"YYYY-MM-DD-HH.MM.SS.NNNNNN", "yyyy-MM-dd-HH.mm.ss.SSSSSS"},
			                                                                 {"DDMMYYYY", "ddMMyyyy"},
			                                                                 {"YYYYDDD", "yyyyDDD"},
			                                                                 {"DDDYYYY", "DDDyyyy"},
			                                                                 {"DD/MM/YY", "dd/MM/yy"},
			                                                                 {"YYYY-MM-DD-HH.MM.SS.mmmmmm", "yyyy-MM-dd-HH.mm.ss.SSSSSS"},
			                                                                 {"00YYYYMMDD", "'00'yyyyMMdd"}};
	
    static Map<String, String> replacerMap = null;

    static {
        replacerMap = new HashMap<String, String>();
        replacerMap.put(UobConstants.SPACE, UobConstants.DOUBLE_UNDERSCORE);
        replacerMap.put(UobConstants.COLON, UobConstants.DOUBLE_PIPE_DELIMITER);
        
        for (String[] validDateFormat : validDateFormats) {
    			VALID_DATE_FORMATS.put(validDateFormat[0], validDateFormat[1]);
    		}
    }
    
    /**
     * This method is used to read a date string with any given date format.
     * @param inputDate The date string to be parsed
     * @param inputDateFormat The date format of the input date string
     * @return the date in the yyyy-MM-dd format
     * @throws EDAGValidationException 
     * @throws ParseException when the date string cannot be parsed with the given date format.
     */
    private static String getFormattedDateInt(String inputDate, String inputDateFormat, boolean prefixWithZeroes) throws EDAGValidationException {
      String dateFormat = null;
      if (StringUtils.isEmpty(inputDateFormat)) {
        return inputDate;
      } else {
      	dateFormat = VALID_DATE_FORMATS.get(inputDateFormat.toUpperCase());
      	
      	if (dateFormat == null) {
      		dateFormat = VALID_DATE_FORMATS.get(inputDateFormat);
      	}
      	
        if (dateFormat == null) {
          dateFormat = inputDateFormat;
        }
      }
      
      // replace double single quotes with single quotes
      String tempFormat = dateFormat.replaceAll("''", "'");
      // then count single quotes in the format
      Matcher matcher = Pattern.compile("'").matcher(tempFormat);
      int quoteCount = 0;
      while (matcher.find()) {
      	quoteCount++;
      }

      // TODO EDF-157
      Date date = null;
  		try {
  			while (prefixWithZeroes && inputDate.length() < dateFormat.length() - quoteCount) {
  				inputDate = "0" + inputDate;
  			}
  			
  			date = new SimpleDateFormat(dateFormat).parse(inputDate);
  		} catch (ParseException e) {
  			throw new EDAGValidationException(EDAGValidationException.INVALID_DATE_FORMAT, inputDate, dateFormat, e.getMessage());
  		}
  		
  		return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }
    
    public static String getFormattedDate(String inputDate, String inputDateFormat, boolean useExternalizedFormat, boolean prefixWithZeroes) throws EDAGValidationException {
    	return useExternalizedFormat ? getFormattedDateExt(inputDate, inputDateFormat, prefixWithZeroes) 
    			                         : getFormattedDateInt(inputDate, inputDateFormat, prefixWithZeroes);
    }
    
    public static String getFormattedDate(String inputDate, String inputDateFormat, boolean prefixWithZeroes) throws EDAGValidationException {
    	return getFormattedDate(inputDate, inputDateFormat, Boolean.parseBoolean(PropertyLoader.getProperty(DateUtils.class.getName() + "." + USE_EXTERNALIZED_FORMAT)), prefixWithZeroes);
    }

    private static String getFormattedDateExt(String inputDate, String inputDateFormat, boolean prefixWithZeroes) throws EDAGValidationException {
        String formattedFieldValue = null;
        String dateFormat = null;
        if (inputDateFormat == null || "".equalsIgnoreCase(inputDateFormat)) {
            return inputDate;
        } else {
            dateFormat = PropertyLoader.getProperty(DateUtils.class.getName() + "." + 
                                                    com.uob.edag.utils.StringUtils.replaceAll(replacerMap, inputDateFormat));
            if (org.apache.commons.lang3.StringUtils.isEmpty(dateFormat)) {
                dateFormat = inputDateFormat;
            }
        }
        SimpleDateFormat dateFormatSdf = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat formatter = new SimpleDateFormat(dateFormat);
        Date date;
        
        // replace double single quotes with single quotes
        String tempFormat = dateFormat.replaceAll("''", "'");
        // then count single quotes in the format
        Matcher matcher = Pattern.compile("'").matcher(tempFormat);
        int quoteCount = 0;
        while (matcher.find()) {
        	quoteCount++;
        }
        
        // TODO EDF-157
        while (prefixWithZeroes && inputDate.length() < dateFormat.length() - quoteCount) {
        	inputDate = "0" + inputDate;
        }
        
				try {
					date = formatter.parse(inputDate);
				} catch (ParseException e) {
					throw new EDAGValidationException(EDAGValidationException.INVALID_DATE_FORMAT, inputDate, dateFormat, e.getMessage());
				}
        formattedFieldValue = dateFormatSdf.format(date);
        return formattedFieldValue;
    }
}
