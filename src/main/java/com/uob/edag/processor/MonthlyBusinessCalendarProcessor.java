package com.uob.edag.processor;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.CalendarDao;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.CalendarModel;
import com.uob.edag.utils.UobUtils;


/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 02/10/2017
 * @Description : The class covers the workflow of the 
 *     Monthly Business Calendar Maintenance Process.
 * 
 */

public class MonthlyBusinessCalendarProcessor {
  private static Options options = new Options();
  
  protected Logger logger = Logger.getLogger(getClass());
  private CalendarDao calendarDao = new CalendarDao();

  /**
   * This is the main class of the Business Calendar Maintenance Process. 
   * This process retrieves the holiday list and updates the business date for processing.
   * @param arguments Valid arguments are -h, -c
   * @throws EDAGValidationException 
   * @throws EDAGMyBatisException 
   * @throws Exception when any error occurs in the Business Calendar Process.
   */
  public static void main(String[] arguments) throws EDAGProcessorException, EDAGMyBatisException, EDAGValidationException {
    CommandLineParser parser = new DefaultParser();
    try {
      options.addOption("h", "help", false, "Show Help");

      Option inOpt = new Option("c", "calendar", false, "Run Business Calendar Process for given Parameters");
      inOpt.setArgs(2);
      inOpt.setArgName("Country> <Biz Date");
      options.addOption(inOpt);

      CommandLine command = parser.parse(options, arguments);

      if (command.hasOption("h")) {
        showHelp();
      }
        
      if (!command.hasOption("c")) {
        throw new EDAGProcessorException(EDAGProcessorException.MISSING_MANDATORY_OPTION, "c", "Calendar Option is mandatory");
      }
      
      String[] args = command.getOptionValues("c");
      if (args == null || args.length < 2) {
        throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "c", "Not enough arguments passed to run Monthly Business Calendar");
      }
      
      String ctryCd = args[0];
      String bizDate = args[1];
      
      String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
      String logFileName = "EDA_DL" + "_MonthlyCalendar_" + ctryCd + "_" + execDate + ".log";
      System.setProperty("logFileName", logFileName);
      UobUtils.logJavaProperties();
      UobUtils.logPackageProperties();
      MonthlyBusinessCalendarProcessor bcProc = new MonthlyBusinessCalendarProcessor();
      bcProc.runMonthlyCalendarRollover(ctryCd, bizDate);
    } catch (ParseException excp) {
      throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, excp.getMessage());
    }
  }
  
  /**
   * This method displays the Help for the Usage of this Java class.
   */
  public static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("MonthlyBusinessCalendarProcessor", options);
    System.exit(0);
  }
  
  /**
   * This method is used to set the business calendar for daily jobs.
   * @throws EDAGMyBatisException 
   * @throws EDAGValidationException 
   * @throws Exception when any error occurs in the Business Calendar Process.
   */
  public void runMonthlyCalendarRollover(String ctryCd, String bizDate) throws EDAGMyBatisException, EDAGValidationException {
    logger.info("Going to perform Monthly Calendar Rollover for country: " + ctryCd);
    List<CalendarModel> list = calendarDao.getCurrentCalendarEntries();
    Map<String, Map<String, Map<String, CalendarModel>>> calendarMap = new HashMap<String, Map<String, Map<String, CalendarModel>>>();
    
    for (CalendarModel model: list) {
      String sourceSystem = model.getSrcSysCd();
      String country = model.getCtryCd();
      String frequency = model.getFreqCd();
      if (calendarMap.containsKey(country)) {
        Map<String, Map<String, CalendarModel>> srcSystemMap = calendarMap.get(country);
        if (srcSystemMap.containsKey(sourceSystem)) {
          Map<String, CalendarModel> freqMap = srcSystemMap.get(sourceSystem);
          if (freqMap.containsKey(frequency)) {
            throw new EDAGValidationException("Duplicate Entry: Freq: " + frequency + ", Country: " + country + 
            		                              ", Source System: " + sourceSystem);
          } else {
            freqMap.put(frequency, model);
          }
        } else {
          Map<String, CalendarModel> freqMap = new HashMap<String, CalendarModel>();
          freqMap.put(frequency, model);
          srcSystemMap.put(sourceSystem, freqMap);  
        }
        
        calendarMap.put(country, srcSystemMap);
      } else {
        Map<String, CalendarModel> freqMap = new HashMap<String, CalendarModel>();
        freqMap.put(frequency, model);
        Map<String, Map<String, CalendarModel>> srcSystemMap = new HashMap<String, Map<String, CalendarModel>>();
        srcSystemMap.put(sourceSystem, freqMap);
        calendarMap.put(country, srcSystemMap);
      } 
    }
    
    if (calendarMap.containsKey(ctryCd)) {
      Map<String, Map<String, CalendarModel>> srcSystemMap = calendarMap.get(ctryCd);
      // Loop for each source system
      for (Entry<String, Map<String, CalendarModel>> entryMap : srcSystemMap.entrySet()) {
        String sourceSystem = entryMap.getKey();
        Map<String, CalendarModel> freqMap = entryMap.getValue();
        if (freqMap.containsKey(UobConstants.MONTHLY_CD)) {
        	String currBizDate = freqMap.containsKey(UobConstants.DAILY_CD) ? freqMap.get(UobConstants.DAILY_CD).getCurrBizDate() 
        			                                                            : bizDate;
        	logger.debug("Daily Current Biz Date for Source System: " + sourceSystem + " is: " + currBizDate +
  		                 ", going to set Monthly Curr Biz Date to the same value");
          
          CalendarModel monthlyModel = freqMap.get(UobConstants.MONTHLY_CD);
          String currMonthlyBizDate = monthlyModel.getCurrBizDate();
          String prevMonthlyBizDate = currMonthlyBizDate;
          currMonthlyBizDate = currBizDate;
          monthlyModel.setCurrBizDate(currMonthlyBizDate);
          monthlyModel.setPrevBizDate(prevMonthlyBizDate);
					calendarDao.updateMonthlyCalendarEntry(monthlyModel);
        } else {
          logger.info("No monthly entries to update for source system: " + sourceSystem);
        }
      }
    } else {
      logger.info("No monthly entries to update for country: " + ctryCd);
    }
  }
}

