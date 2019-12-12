package com.uob.edag.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGPOIException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.SparkExecParamsModel;

public class UobExecParamsRegParser {

    protected Logger logger = Logger.getLogger(getClass());
    private static final List<String> VALID_SHEET_NAMES = new ArrayList<String>();

    static {
        VALID_SHEET_NAMES.add(UobConstants.SPARK_EXEC_PARAMS_SHEET);
        VALID_SHEET_NAMES.add(UobConstants.SPARK_BASED_INGESTION_SHEET);
        VALID_SHEET_NAMES.add(UobConstants.PRE_PROCESS_FLAG_SHEET);
        VALID_SHEET_NAMES.add(UobConstants.PRE_PROCESS_PROC_PARAM_SHEET);
    }

    private static final Map<String,String> SHEET_TO_FILE_MAP = new HashMap<String, String>();
    static {
        SHEET_TO_FILE_MAP.put(UobConstants.SPARK_EXEC_PARAMS_SHEET+"_MI",UobConstants.SPARK_EXEC_PARAMS_MI_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.SPARK_EXEC_PARAMS_SHEET+"_MD",UobConstants.SPARK_EXEC_PARAMS_MD_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.SPARK_BASED_INGESTION_SHEET+"_MI",UobConstants.SPARK_BASED_INGESTION_MI_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.SPARK_BASED_INGESTION_SHEET+"_MD",UobConstants.SPARK_BASED_INGESTION_MD_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.PRE_PROCESS_FLAG_SHEET+"_MI",UobConstants.PRE_PROCESS_FLAG_MI_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.PRE_PROCESS_FLAG_SHEET+"_MD",UobConstants.PRE_PROCESS_FLAG_MD_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.PRE_PROCESS_PROC_PARAM_SHEET+"_MI",UobConstants.PRE_PROCESS_PROC_PARAM_MI_FILE);
        SHEET_TO_FILE_MAP.put(UobConstants.PRE_PROCESS_PROC_PARAM_SHEET+"_MD",UobConstants.PRE_PROCESS_PROC_PARAM_MD_FILE);
    }

    static {
        VALID_SHEET_NAMES.add(UobConstants.SPARK_EXEC_PARAMS_SHEET);
        VALID_SHEET_NAMES.add(UobConstants.SPARK_BASED_INGESTION_SHEET);
        VALID_SHEET_NAMES.add(UobConstants.PRE_PROCESS_FLAG_SHEET);
        VALID_SHEET_NAMES.add(UobConstants.PRE_PROCESS_PROC_PARAM_SHEET);
    }

    private static final MessageFormat INSERT_SPARK_BASED_INGESTION_TEMPLATE = new MessageFormat(
            UobUtils.ltrim("UPDATE EDAG_FILE_DETAIL SET USE_SPARK_BASED_INGESTION = ''Y'' WHERE PROC_ID=''{0}'';")
    );

    private static final MessageFormat REMOVE_SPARK_BASED_INGESTION_TEMPLATE = new MessageFormat(
            "UPDATE EDAG_FILE_DETAIL SET USE_SPARK_BASED_INGESTION = ''N'' WHERE PROC_ID=''{0}'';"
    );

    private static final MessageFormat INSERT_PRE_PROCESS_FLAG_TEMPLATE = new MessageFormat(
            UobUtils.ltrim("INSERT INTO EDAG_PROC_PRE_PROCESS_FLAG(PROC_ID, CTRY_CD, FLG) ") +
                    UobUtils.ltrim(" VALUES(''{0}'', ''{1}'', ''{2}''); ")
    );

    private static final MessageFormat REMOVE_PRE_PROCESS_FLAG_TEMPLATE = new MessageFormat(
            "DELETE FROM EDAG_PROC_PRE_PROCESS_FLAG WHERE PROC_ID = ''{0}'' AND CTRY_CD=''{1}''; "
    );

    private static final MessageFormat INSERT_PRE_PROCESS_PROC_PARAM_TEMPLATE = new MessageFormat(
            UobUtils.ltrim("INSERT INTO EDAG_PROC_PARAM(PROC_ID, PARAM_NM, PARAM_VAL) ") +
                    UobUtils.ltrim(" VALUES(''{0}'', ''{1}'', ''{2}''); ")
    );

    private static final MessageFormat REMOVE_PRE_PROCESS_PROC_PARAM_TEMPLATE = new MessageFormat(
            "DELETE FROM EDAG_PROC_PARAM WHERE PROC_ID = ''{0}'' AND PARAM_NM=''{1}''; "
    );

    public String getInsertSparkBasedIngestionSql(String procId, String flag) {
        String result = INSERT_SPARK_BASED_INGESTION_TEMPLATE.format(new Object[] {procId, flag});
        logger.debug("Insert to Spark Based Ingestion SQL: " + result);
        return result;
    }

    public String getRemoveSparkBasedIngestionSql(String procId) {
        String result = REMOVE_SPARK_BASED_INGESTION_TEMPLATE.format(new Object[] {procId});
        logger.debug("Remove Spark Exec Params statement: " + result);
        return result;
    }

    public String getInsertPreProcessFlagSql(String procId, String ctryCd, String flag) {
        String result = INSERT_PRE_PROCESS_FLAG_TEMPLATE.format(new Object[] {procId, ctryCd, flag});
        logger.debug("Insert to Spark Exec Params SQL: " + result);
        return result;
    }

    public String getRemovePreProcessFlagSql(String procId, String ctryCd) {
        String result = REMOVE_PRE_PROCESS_FLAG_TEMPLATE.format(new Object[] {procId, ctryCd});
        logger.debug("Remove Spark Exec Params statement: " + result);
        return result;
    }

    public String getInsertPreProcessProcParamSql(String procId, String paramNm, String paramVal) {
        String result = INSERT_PRE_PROCESS_PROC_PARAM_TEMPLATE.format(new Object[] {procId, paramNm, paramVal});
        logger.debug("Insert to Spark Exec Params SQL: " + result);
        return result;
    }

    public String getRemovePreProcessProcParamSql(String procId, String paramNm) {
        String result = REMOVE_PRE_PROCESS_PROC_PARAM_TEMPLATE.format(new Object[] {procId, paramNm});
        logger.debug("Remove Spark Exec Params statement: " + result);
        return result;
    }

    public Map < String, ArrayList < String >> checkAndParseSheets(String regFilePath) throws EDAGPOIException, EDAGValidationException {
        Map <String, ArrayList <String>> allRegistrationSQLs = null;
        FileInputStream fis = null;
        Workbook workbook = null;

        try {
            fis = new FileInputStream(regFilePath);
            workbook = new XSSFWorkbook(fis);
            allRegistrationSQLs = new HashMap <String, ArrayList <String>> ();
            logger.info("Going to parse Spark Exec Params Sheet: " + regFilePath);
        } catch (IOException e) {
            throw new EDAGPOIException(EDAGPOIException.CANNOT_OPEN_EXCEL_WORKBOOK, regFilePath, e.getMessage());
        }

        try {
            int noOfSheets = workbook.getNumberOfSheets();
            for(String sheetName : this.VALID_SHEET_NAMES) {
                //Parse 'Spark Exec Params' sheet
                Sheet sheet = workbook.getSheet(sheetName);
                if (sheet == null) {
                    logger.info("There is no \"" + sheetName+ "\" Sheet in the file provided.");
                    continue;
                }

                int noOfRows = getNumberOfRows(sheet);
                logger.debug("Number of Rows in Sheet: " + noOfRows);
                System.out.println("Number of Rows in Sheet: " + noOfRows);
                allRegistrationSQLs.putAll(parseMetadataRegistrationExcelSheets(sheet, noOfRows));
            }
        } finally {
                try {
                    workbook.close();
                } catch (IOException e) {
                    logger.warn("Unable to close workbook " + regFilePath + ": " + e.getMessage());
                }
            }
            return allRegistrationSQLs;
    }

    public Map < String, ArrayList < String >> parseMetadataRegistrationExcelSheets(Sheet sheet, int noOfRows) throws EDAGPOIException, EDAGValidationException {

        Map <String, ArrayList <String>> sparkExecParamsInfo = null;
        List <String> insertLine = null;
        List <String> deleteLine = null;

        int row = 0;
        int col = 0;

        int lastProcessedRow = 0;
        int processedRowCount = 0;

        try {

            insertLine = new ArrayList <String> ();
            deleteLine = new ArrayList <String> ();

            insertLine.add("set define off;");
            insertLine.add("whenever SQLERROR EXIT ROLLBACK;");

            deleteLine.add("set define off;");
            deleteLine.add("whenever SQLERROR EXIT ROLLBACK;");

            for (row = 0; row <= noOfRows; row++) {
                col = 0;
                if (row < 1) {
                    continue; // Header Line
                } else {
                    Row wbRow = sheet.getRow(row);
                    if (wbRow == null) {
                        logger.debug("Skipping Empty Row: " + row);
                        continue;
                    }
                }

                if(sheet.getSheetName().equalsIgnoreCase(UobConstants.SPARK_EXEC_PARAMS_SHEET)) {
                    String procId = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String ctryCd = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String paramNm = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String driverMem = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String executorMem = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String noOfExec = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String executorCores = POIUtils.getCellContent(sheet, row, col++, String.class);

                    SparkExecParamsModel sparkExecParamsModel = new SparkExecParamsModel();

                    sparkExecParamsModel.setProcId(procId);
                    sparkExecParamsModel.setCtryCd(ctryCd);
                    sparkExecParamsModel.setParamNm(paramNm);
                    sparkExecParamsModel.setDriverMemory(driverMem);
                    sparkExecParamsModel.setExecutorMemory(executorMem);
                    sparkExecParamsModel.setExecutorInstances(noOfExec);
                    sparkExecParamsModel.setExecutorCores(executorCores);

                    //construct INSERT SQL
                    insertLine.add(sparkExecParamsModel.getInsertProcessMasterSql());

                    //construct DELETE SQL
                    deleteLine.add(sparkExecParamsModel.getRemoveProcessMasterSql());
                }
                else if(sheet.getSheetName().equalsIgnoreCase(UobConstants.SPARK_BASED_INGESTION_SHEET)) {
                    String procId = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String flag = POIUtils.getCellContent(sheet, row, col++, String.class);

                    //construct INSERT SQL
                    insertLine.add(getInsertSparkBasedIngestionSql(procId,flag));

                    //construct DELETE SQL
                    deleteLine.add(getRemoveSparkBasedIngestionSql(procId));
                }
                else if(sheet.getSheetName().equalsIgnoreCase(UobConstants.PRE_PROCESS_FLAG_SHEET)) {
                    String procId = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String ctryCd = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String flag = POIUtils.getCellContent(sheet, row, col++, String.class);

                    //construct INSERT SQL
                    insertLine.add(getInsertPreProcessFlagSql(procId,ctryCd,flag));

                    //construct DELETE SQL
                    deleteLine.add(getRemovePreProcessFlagSql(procId,ctryCd));
                }
                else if(sheet.getSheetName().equalsIgnoreCase(UobConstants.PRE_PROCESS_PROC_PARAM_SHEET)) {
                    String procId = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String paramNm = POIUtils.getCellContent(sheet, row, col++, String.class);
                    String paramVal = POIUtils.getCellContent(sheet, row, col++, String.class);

                    //construct INSERT SQL
                    insertLine.add(getInsertPreProcessProcParamSql(procId,paramNm,paramVal));

                    //construct DELETE SQL
                    deleteLine.add(getRemovePreProcessProcParamSql(procId,paramNm));
                }
                lastProcessedRow = row + 1;
                processedRowCount++;

            }

            insertLine.add("COMMIT; ");
            deleteLine.add("COMMIT; ");
            sparkExecParamsInfo = new HashMap <String, ArrayList <String>> ();
            sparkExecParamsInfo.put(SHEET_TO_FILE_MAP.get(sheet.getSheetName()+"_MI"), (ArrayList <String> ) insertLine);
            sparkExecParamsInfo.put(SHEET_TO_FILE_MAP.get(sheet.getSheetName()+"_MD"), (ArrayList <String> ) deleteLine);

            return sparkExecParamsInfo;
        } finally {
            logger.debug(processedRowCount + " parsed successfully from "+ sheet.getSheetName() +" Sheet, last processed row number is " + lastProcessedRow);
        }
    }

    private int getNumberOfRows(Sheet sheet) {
        return sheet.getLastRowNum();
    }

}