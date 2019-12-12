package com.uob.edag.processor;

import java.text.SimpleDateFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.uob.edag.dao.CalendarDao;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.utils.UobUtils;


/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 02/09/2017
 * @Description : The class covers the workflow of the Business Calendar Maintenance Process.
 * 
 */

public class DailyBusinessCalendarProcessor {
  private static Options options = new Options();
  
  protected Logger logger = Logger.getLogger(getClass());
  private CalendarDao calendarDao = new CalendarDao();

  /**
   * This is the main class of the Business Calendar Maintenance Process. 
   * This process retrieves the holiday list and updates the business date for processing.
   * @param arguments Valid arguments are -h, -c
   * @throws Exception when any error occurs in the Business Calendar Process.
   */
  public static void main(String[] arguments) throws EDAGProcessorException {
    CommandLineParser parser = new DefaultParser();
    try {
      options.addOption("h", "help", false, "Show Help");

      Option inOpt = new Option("c", "calendar", false, "Run Business Calendar Process for given Parameters");
      options.addOption(inOpt);

      CommandLine command = parser.parse(options, arguments);

      if (command.hasOption("h")) {
        showHelp();
      }
        
      if (!command.hasOption("c")) {
        throw new EDAGProcessorException("Calendar Option is mandatory");
      }
      
      String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
      String logFileName = "EDA_DL" + "_DailyCalendar_" + execDate + ".log";
      System.setProperty("logFileName", logFileName);
      UobUtils.logJavaProperties();
      UobUtils.logPackageProperties();
      DailyBusinessCalendarProcessor bcProc = new DailyBusinessCalendarProcessor();
      bcProc.runDailyCalendarRollover();
    } catch (ParseException | EDAGMyBatisException excp) {
      throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, excp.getMessage());
    }
  }
  
  /**
   * This method displays the Help for the Usage of this Java class.
   */
  public static void showHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("DailyBusinessCalendarProcessor", options);
    System.exit(0);
  }
  
  /**
   * This method is used to set the business calendar for daily jobs.
   * @throws Exception when any error occurs in the Business Calendar Process.
   */
  public void runDailyCalendarRollover() throws EDAGMyBatisException {
    calendarDao.rollOverDailyCalendar();
    calendarDao.updateProcFlagDailyY();
    calendarDao.updateProcFlagDailyN();
  }
}

