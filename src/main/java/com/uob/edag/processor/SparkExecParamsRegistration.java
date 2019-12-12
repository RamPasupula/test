package com.uob.edag.processor;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.FileUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobExecParamsRegParser;

public class SparkExecParamsRegistration {

  private static Logger logger = null;

  public static void main(String[] args) throws EncryptedDocumentException, InvalidFormatException, EDAGProcessorException, EDAGValidationException, EDAGIOException, IOException {

    //Check if user indicated the source file
    if (args.length == 0) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Export Excel File Path", "Export Excel File Path cannot be empty");
    }

    String regFilePath = args[0];

    //Create logs
    File regFileSpec = new File(regFilePath);
    String fileName = regFileSpec.getName();
    String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
    String logFileName = "EDA_REG_" + fileName + "_" + execDate + ".log";
    System.setProperty("logFileName", logFileName);
    logger = Logger.getLogger(SparkExecParamsRegistration.class);
    logger.setLevel(Level.INFO);
    logger.info("Start registration of execution parameters for metadata.");

    System.out.println("First argument:" + args[0]);

    SparkExecParamsRegistration reg = new SparkExecParamsRegistration();
    reg.processRegistrationExport(regFilePath);
  }

  private void processRegistrationExport(String regFilePath) throws EDAGProcessorException, EDAGValidationException, EDAGIOException, EncryptedDocumentException, InvalidFormatException, IOException {
    logger.info("Start registration of execution parameters for metadata: " + regFilePath);

    Map <String, EDAGException> errorProcesses = new HashMap < String, EDAGException > ();

    UobExecParamsRegParser parser = new UobExecParamsRegParser();
    // Parse the Export Process Sheet
    Map <String, ArrayList<String>> mapSparkExecParamsProc = parser.checkAndParseSheets(regFilePath);
    //create SQL files
    createRegExportSqlFile(mapSparkExecParamsProc);
  }

  private void createRegExportSqlFile(Map < String, ArrayList < String >> mapSparkExecParamsProc) throws EDAGIOException {
    String sqlFileLocation = PropertyLoader.getProperty(UobConstants.SQL_FILE_LOC);
    String dataDir = PropertyLoader.getProperty(UobConstants.DATADIR);
    sqlFileLocation = sqlFileLocation.replace(UobConstants.DATADIR_STR_PARAM, dataDir);

    for (Entry < String, ArrayList < String >> entrySet: mapSparkExecParamsProc.entrySet()) {
      String sqlFileName = entrySet.getKey();
      String fullFileName = sqlFileLocation + File.separator + sqlFileName;
      ArrayList < String > sqlArrList = new ArrayList < String > ();

      sqlArrList.addAll(entrySet.getValue());
      File sqlFile = new File(fullFileName);

      if (sqlFile.exists()) {
        //delete existing
      }

      FileUtils fileUtil = new FileUtils();
      fileUtil.writeToFile(sqlArrList, sqlFile);
      logger.info("New SQL file created successfully:" + fullFileName);
      System.out.println("New SQL file created successfully:" + fullFileName);
    }

  }
}