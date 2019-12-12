package com.uob.edag.processor;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.uob.edag.exception.EDAGPOIException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;

import com.google.common.base.Throwables;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGSQLException;
import com.uob.edag.model.InterfaceSpec;
import com.uob.edag.security.EncryptionUtil;

/**
 * <h3>Overview</h3>
 * Class to accompany the Registration Generator Class for handling the PII information.
 * This class is triggered only if the -p flag is enabled from the standard input.
 *
 * This processing class reads PII information from the PII column of the
 * File Specification Excel File and generates the Hive Views for Sensitive and Non-Sensitives.
 *
 *
 * <ul>
 * <li><b>Sensitive Views</b>
 * <br>Views that contain Personally Identifiable Information (PII)</li>
 *
 * <li><b>Non-Sensitive Views</b>
 * <br>Views that have PII fields masked as NULL.</li>
 *
 *
 * </ul>
 * <h3>Processing Logic</h3>
 *
 * <i>Stepwise flow in code comments</i>
 * @see PIIRegistrationProcessor#processPIIFromFileSpec
 *
 * <ul>
 *      <li><b>Metadata Registration</b>
 *          <ul>
 *
 *              <li>
 *                  Get a list of valid filenames. The filenames are tabs in the source filespec.
 *                  @see PIIRegistrationProcessor#getValidFileNames()
 *
 *                  This is done for handling:
 *                  <ol>
 *                      <li> When one process spec is used for different filespec.</li>
 *                      <li> When control file is also present, alongwith regular file.</li>
 *                  </ol>
 *              </li>
 *              <li>
 *                  Check if prior entries exist in the table EDAG_FIELD_SENSITIVE_DETAIL for current process ID.
 *                  @see PIIRegistrationProcessor#preRunHouseKeeping(String fileName)
 *              </li>
 *
 *              <li>
 *                  If, yes, delete those entries.
 *                  @see PIIRegistrationProcessor#deleteExistingRecords()
 *              </li>
 *              <li>
 *                  Read from the File Specification Excel document the column for PII information
 *                  @see PIIRegistrationProcessor#readPIIFieldsFromFileSpec(String fileName, String piiCol)
 *              </li>
 *
 *              <li>
 *                  Check if any specific field needs to be suppressed, remove them from target view list.
 *                  The fields must be the same as the one in File Spec Excel and not normalized names.
 *                  @see PIIRegistrationProcessor#extractFieldSpec(String fileName)
 *              </li>
 *
 *              <li>Process only Columns marked with 'Y' or 'y'</li>
 *
 *              <li>From the Interface Spec Object get the following information
 *                  <table>
 *                      <caption>PII Specific Object Derivations</caption>
 *                      <tr>
 *                          <th><b>Registration Field</b></th>
 *                          <th><b>Field Description</b></th>
 *                          <th><b>Derived During Registration</b></th>
 *                          <th><b>Post Registration</b></th>
 *                      </tr>
 *                      <tr>
 *                          <td>ctry_cd</td>
 *                          <td>country code</td>
 *                          <td>
 *                              Extract Process Spec Obj Process Spec Object
 *                              @see PIIRegistrationProcessor#extractProcessSpec(String fileName)
 *                          </td>
 *                          <td>edag_process_master</td>
 *                      </tr>
 *                      <tr>
 *                          <td>tbl_nm</td>
 *                          <td>table name</td>
 *                          <td>
 *                              Extract Process Spec Obj Process Spec Object
 *                              @see PIIRegistrationProcessor#extractProcessSpec(String fileName)
 *                          </td>
 *                          <td>edag_load_process</td>
 *                      </tr>
 *                      <tr>
 *                          <td>fld_nm</td>
 *                          <td>field name</td>
 *                          <td>
 *                              Extract Field Spec Obj Source Field Spec Object
 *                              @see PIIRegistrationProcessor#extractFieldSpec(String fileName)
 *                          </td>
 *                          <td>edag_field_detail</td>
 *                      </tr>
 *                      <tr>
 *                          <td>fld_num</td>
 *                          <td>field num</td>
 *                          <td>
 *                              Source Field Spec Object
 *                              @see PIIRegistrationProcessor#extractFieldSpec(String fileName)
 *                          </td>
 *                          <td>edag_field_detail</td>
 *                      </tr>
 *                      <tr>
 *                          <td>fld_desc</td>
 *                          <td>field description</td>
 *                          <td>
 *                              Source Field Spec Object
 *                              @see PIIRegistrationProcessor#extractFieldSpec(String fileName)
 *                          </td>
 *                          <td>edag_field_detail</td>
 *                      </tr>
 *                      <tr>
 *                          <td>tgt_aply_type_cd</td>
 *                          <td>target apply type cd</td>
 *                          <td>
 *                              Extract File Spec Obj Source File Spec Object
 *                              @see PIIRegistrationProcessor#extractFileSpec(String fileName)
 *                          </td>
 *                          <td>edag_load_process</td>
 *                      </tr>
 *                  </table>
 *              </li>
 *          </ul>
 *      </li>
 *
 *
 *      <li><b>HQL Generation</b>
 *          <ul>
 *              <li>
 *                  For Non-sensitive views mark the sensitive columns as &lt;NULL&gt;
 *                  @see PIIRegistrationProcessor#extractFieldSpec(ArrayList, String)
 *              </li>
 *              <li>
 *                  Get the target country specific DBs
 *                  @see PIIRegistrationProcessor#getApplicableDatabases(String, ArrayList, String)
 *              </li>
 *              <li>
 *                  Use the Mapper Template to generate the views
 *                  @see PIIRegistrationProcessor#prepareHQL(ConcurrentHashMap, ConcurrentHashMap, String, String, ArrayList)
 *              </li>
 *              <li>
 *                  Write HQL non-sensitive and sensitive views.
 *                  @see PIIRegistrationProcessor#writeHQLToFile(String, String)
 *              </li>
 *          </ul>
 *      </li>
 *
 *      <li>Technical Specification
 *          (see <a href="https://thinkbiganalytics.atlassian.net/browse/EDF-224">EDF-224</a>)
 *      </li>
 *
 * </ul>
 *
 *
 * @author     Subhadip Mitra
 * @version     %I%, %G%
 * @since       1.0
 */
public class PIIRegistrationProcessor {

    private static String srcSystem;
    private InterfaceSpec spec;
    private String interfaceSpecFilePath;
    private String interfaceSpecFileName;
    private String processSpecFilePath;
    private String processSpecFileName;
    private static Connection connection = null;
    private String outfilePath;
    private String outfilename;
    private Logger logger;

    private static String PII_FLAG; // Value to look for in the PII Applicability Column
    private static String BATCH_NUM;

    private static boolean ENABLE_SUPPRESS_HQL_FIELD;
    private static List<String> SUPPRESSED_HQL_FIELDS;
    private static List<String> SUPPRESSED_HQL_FIELDS_AND_DESC_MATCHED;

    // Prepared Statements
    private static String PS_DELETE_EXISTING_PII;
    private static String PS_DELETE_EXISTING_PII_WITHOUT_FILENAME;
    private static String PS_PRECHECK_EXISTS_PII;
    private static String PS_INSERT_SENSITIVE_TEMPLATE;
    private static String VIEW_TEMPLATE_PROPERTY;

    private static String NULL_TAG;
    private static String CTRY;
    private static String IS_TRUE;
    private static String NS_VIEWS_OPTION;
    private static String S_VIEWS_OPTION;
    private static String CURRENT_TIMESTAMP;

    // Source File Spec Specific
    private static int FILE_SPEC_FILE_DESCRIPTION_COL_NUM;
    private static int FILE_SPEC_FILE_TYPE_COL_NUM;
    private static int FILE_SPEC_FILE_NAME_COL_NUM;

    private static String FILE_NAME_ROW_TYPE_FIELD_VALUE_HEADER;
    private static String FILE_NAME_ROW_TYPE_FIELD_VALUE_FOOTER;
    private static int FILE_NAME_FIELD_NAME_COL_NUM;
    private static int FILE_NAME_ROW_TYPE_COL_NUM;

    private static StringBuilder nsHQL= new StringBuilder();
    private static StringBuilder sHQL= new StringBuilder();
    private static String LOGGER_TAG;

    // Added to optionally suppress the comments.
    private boolean enableSQLComments;
    private boolean generateCleanupScript;
    private String cleanupFileName;
    private StringBuilder drop_views = new StringBuilder(UobConstants.EMPTY);


    private static String FILENAME_CTRY_REGEX;

    /**
     * Default Constructor
     * @param spec Interface Spec Obj
     * @param interfaceSpecFilePath Absolute path to Interface Spec File
     * @param logger the Logger Obj
     * @throws EDAGException wrapped ClassNotFound and SQLException
     */

    public PIIRegistrationProcessor(InterfaceSpec spec, String interfaceSpecFilePath, String processSpecFilePath, Logger logger) throws EDAGException {

        setOutfilePath(PropertyLoader
                .getProperty(UobConstants.SQL_FILE_LOC));

        setOutfilename(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName() + UobConstants.SQL_FILE_DDS_NAME));

        setEnableSuppressHqlField(Boolean.parseBoolean(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName() + UobConstants.ENABLE_SUPPRESS_HQL_FIELD)));

        setSuppressedHqlFields(Arrays.asList(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName() + UobConstants.SUPPRESSED_HQL_FIELDS)
                .split("\\s*,\\s*")));

        setBatchNum(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName() + UobConstants.BATCH_NUM));

        setPiiFlag(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName() + UobConstants.PII_FLAG));

        setViewTemplateProperty(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName()+ UobConstants.VIEW_TEMPLATE));

        setEnableSQLComments(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName()+ UobConstants.ENABLE_SQL_COMMENTS));

        setGenerateCleanupScript(Boolean.parseBoolean(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName()+ UobConstants.GENERATE_CLEANUP_SCRIPT)));

        setFilenameCtryRegex(PropertyLoader
                .getProperty(PIIRegistrationProcessor.class.getName()+ UobConstants.FILENAME_CTRY_REGEX));

        setFileSpecFileDescriptionColNum(UobConstants.FILE_SPEC_FILE_DESCRIPTION_COL_NUM);
        setFileSpecFileTypeColNum(UobConstants.FILE_SPEC_FILE_TYPE_COL_NUM);
        setFileSpecFileNameColNum(UobConstants.FILE_SPEC_FILE_NAME_COL_NUM);
        setFileNameFieldNameColNum(UobConstants.FILE_NAME_FIELD_NAME_COL_NUM);
        setFileNameRowTypeColNum(UobConstants.FILE_NAME_ROW_TYPE_COL_NUM);
        setFileNameRowTypeFieldValueHeader(UobConstants.FILE_NAME_ROW_TYPE_FIELD_VALUE_HEADER);
        setFileNameRowTypeFieldValueFooter(UobConstants.FILE_NAME_ROW_TYPE_FIELD_VALUE_FOOTER);
        setCurrentTimestamp(UobConstants.CURRENT_TIMESTAMP);
        setNsViewsOption(UobConstants.NS_VIEWS_OPTION);
        setsViewsOption(UobConstants.S_VIEWS_OPTION);
        setNullTag(UobConstants.NULL_TAG);
        setIsTrue(UobConstants.IS_TRUE);
        setCTRY(UobConstants.CTRY);

        setPsInsertSensitiveTemplate(UobConstants.PS_INSERT_SENSITIVE_TEMPLATE);
        setPsDeleteExistingPii(UobConstants.PS_DELETE_EXISTING_PII);
        setPsDeleteExistingPiiWithoutFilename(UobConstants.PS_DELETE_EXISTING_PII_WITHOUT_FILENAME);
        setPsPrecheckExistsPii(UobConstants.PS_PRECHECK_EXISTS_PII);

        setInterfaceSpecFilePath(interfaceSpecFilePath);
        setProcessSpecFilePath(processSpecFilePath);
        setInterfaceSpecFileName(Paths.get(getInterfaceSpecFilePath()).getFileName().toString());
        setProcessSpecFileName(Paths.get(getProcessSpecFilePath()).getFileName().toString());
        setCleanupFileName(UobConstants.CLEANUP_FILE_NAME);

        setUpJDBCConnection();
        setLogger(logger);
        setLoggerTag();
        setSpec(spec);

    }




    /**
     * Main entry program.
     * Parse the Source Interface File Spec to get the column names and does subsequent processing.
     *
     * @param piiCol the PII Column Number in the FileSpec.
     * @throws EDAGException Generic Exception
     */

    public void processPIIFromFileSpec(String piiCol) throws EDAGException {

        logger.debug(LOGGER_TAG + "Starting PII registration ..");

        setSrcSystem(getSpec().getSrcSystem());

        // STEP 1: get a list of all valid files (except control file)
        ArrayList<String> fileNames = getValidFileNames();
        Map<String, Map<String, InterfaceSpecMap>> allProcessSpecs =  getSpec().getProcessSpec();


        try {
            // STEP 2: Extract the processModel Object.
            for (Map.Entry<String, Map<String, InterfaceSpecMap>> entry : allProcessSpecs.entrySet()) {

                logger.debug(LOGGER_TAG + "processPIIFromFileSpec =>  entry,getKey()" + entry.getKey()
                        + ", interfaceSpecFilePath: " + getInterfaceSpecFileName());
                // For cases where same process spec may have multiple file names and multiple field values.
                if (entry.getKey().equals(getInterfaceSpecFileName())){

                    for (Map.Entry<String, InterfaceSpecMap> p : entry.getValue().entrySet()) {
                        String fileName = p.getKey();
                        setLoggerTag(fileName);

                        logger.debug(LOGGER_TAG + "fileName is :" + p.getKey());

                        if (fileNames.contains(fileName)) { // validate for control file.
                            InterfaceSpecMap ism = p.getValue();

                            String hiveTblNm = ism.get(UobConstants.TABLE_NAME).toString();
                            logger.debug(LOGGER_TAG + "Hive Table Name: " + hiveTblNm);

                            setSrcSystem(ism.get(UobConstants.SRC_SYS_NM).toString());

                            ArrayList<String> piiFields;

                            logger.debug(LOGGER_TAG + "Going to read PII information from FileSpec: fileName="
                                    + fileName + ", piiCol=" + piiCol + "");

                            // STEP 3. Read the File Spec and get the PII Column values. Do not rely on filespec object, helps override.
                            piiFields = readPIIFieldsFromFileSpec(fileName, piiCol);

                            logger.debug(LOGGER_TAG + "PII Fields from FileSpec:" + piiFields);

                            //STEP 4:  Do pre-check that there are no existing entries, if Yes, remove them for this SRC_SYS and fileName
                            logger.debug(LOGGER_TAG + "Clearing old PII field entries from edag_field_sensitive_detail for => (src system:"
                                    + getSrcSystem() +", fileName: " + fileName  + ")");
                            preRunHouseKeeping(fileName);

                            //STEP 5: do Registration.
                            registerPIIFields(hiveTblNm, fileName, piiFields);

                            //STEP 6: Do for Non-Sensitive Views
                            generateHQLViews(getNsViewsOption(), piiFields, fileName);

                            //STEP 7: Do for Sensitive Views
                            generateHQLViews(getsViewsOption(), null, fileName); // for sensitive views no masking needed.


                        }

                    }
                }
            }
            setLoggerTag(); // reset
            writeHQLToFile(getNsHQL().toString(), getNsViewsOption());
            writeHQLToFile(getsHQL().toString(), getsViewsOption());




        } catch (Exception e) {
            logger.error(Throwables.getStackTraceAsString(e));
            throw new EDAGException(LOGGER_TAG + "Encountered Error while trying to read " +
                    "valid filenames from tab File Spec", e);

        }

        // Generate Cleanup File if Enabled.
        if(isGenerateCleanupScript()) writeCleanupFile();

        logger.debug(LOGGER_TAG + "PII registration completed.");
    }

    /**
     * Rollback, delete orphaned records from Sensitve Fields Table.
     */
    public void rollback(){

        if(  getSrcSystem() != null && !getSrcSystem().isEmpty() ) {
            if(deleteExistingRecords() > 0 ) logger.info(LOGGER_TAG + "Successfully Rolled Back, Src System PII:"
                    + getSrcSystem());
            if(deleteExistingRecords() == 0) logger.info(LOGGER_TAG + "RollBack Not needed, no Src Systems were " +
                    "processed for PII");
            if(deleteExistingRecords() < 0) logger.info(LOGGER_TAG + "Failed to Rollback, Src System PII: "
                    + getSrcSystem() + ". Delete Manually.");
        }
        else logger.info("RollBack Not needed, no Src Systems were processed for PII");

    }

    /**
     * Since the same path objs are reused, resetting.
     */
    private void resetPathsContext(){
        logger.debug(LOGGER_TAG + "resetting paths context..");
        setOutfilePath(PropertyLoader.getProperty("SQL_FILE_LOCATION"));
        setOutfilename(PropertyLoader.getProperty(PIIRegistrationProcessor.class.getName() + ".SQL_FILE_DDS_NAME"));
        setCleanupFileName(UobConstants.CLEANUP_FILE_NAME);
    }


    /**
     * Delete Existing Records of PII Fields
     */

    private void deleteExistingRecords(String fileName){
        int num_deleted = -1;

        try {
            PreparedStatement ps_delete_existing = getConnection().prepareStatement(getPsDeleteExistingPii());
            logger.debug(LOGGER_TAG + getPsDeleteExistingPii() + " prepared");
            ps_delete_existing.setString(1, getSrcSystem());
            ps_delete_existing.setString(2, fileName);
            num_deleted = ps_delete_existing.executeUpdate();
            logger.debug(LOGGER_TAG + "Deleted" + num_deleted +" from EDAG_FIELD_SENSITIVE_DETAILS"
                    + "for fileName "+ fileName);
        } catch (SQLException e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
    }


    /**
     * Overloaded.
     * @return deletion was successful
     */
    private int deleteExistingRecords(){
        int num_deleted = -1;

        try {
            PreparedStatement ps_delete_existing = getConnection()
                    .prepareStatement(getPsDeleteExistingPiiWithoutFilename());
            logger.debug(LOGGER_TAG + getPsDeleteExistingPii() + " prepared");
            ps_delete_existing.setString(1, getSrcSystem());
            num_deleted = ps_delete_existing.executeUpdate();
            logger.debug(LOGGER_TAG + "Deleted" + num_deleted +" from EDAG_FIELD_SENSITIVE_DETAILS");
        } catch (SQLException e) {
            logger.error(Throwables.getStackTraceAsString(e));
        }
        return num_deleted;
    }


    /**
     * Check if EDAG_FIELD_SENSITIVE_DETAIL has existing records.
     * @return
     */
    private boolean hasExistingRecords(){
        boolean q_status;
        try {
            PreparedStatement ps_check_records = getConnection()
                    .prepareStatement(getPsPrecheckExistsPii());
            logger.debug(LOGGER_TAG + getPsPrecheckExistsPii() + " prepared");
            ps_check_records.setString(1, getSrcSystem());
            ResultSet rs = ps_check_records.executeQuery();
            logger.debug(LOGGER_TAG + "Pre-existing records were found in Sensitive Fields table.");
            q_status = !rs.wasNull(); // return only for successful query exec.
        } catch (SQLException e) {
            logger.error(LOGGER_TAG + Throwables.getStackTraceAsString(e));
            logger.error(LOGGER_TAG + "Failed to check if there are pre-existing records in Sensitive Fields table.");
            q_status = true;
        }
        return q_status;
    }


    /**
     * Runs a precheck if PII fields already exists for Source System.
     * @return has precheck successfully executed?
     */

    private void preRunHouseKeeping(String fileName) {
        deleteExistingRecords(fileName);
    }


    private void preRunHouseKeeping() {
        deleteExistingRecords();
    }


    /**
     * Get all the valid file names from interface spec minus the control file.
     * This is done to handle the case where one process spec is associated with multiple filespec.
     *
     * @return list filenames contained in the filespec
     * @throws EDAGException multiple exceptions, IOException, POIException, ValidationException
     */

    private ArrayList<String> getValidFileNames() throws EDAGException {

        ArrayList<String> validFileNamesList = new ArrayList<>();

        XSSFWorkbook wb;
        int row;

        try {
            wb = new XSSFWorkbook(new FileInputStream(getInterfaceSpecFilePath()));
        } catch (IOException e) {
            logger.error(LOGGER_TAG + Throwables.getStackTraceAsString(e));
            throw new EDAGException("PII Registration Unable to open workbook "+ getInterfaceSpecFilePath());
        }


        try {

            logger.info(LOGGER_TAG + "PII Registration - Going to parse Source File Info for file: "
                    + getInterfaceSpecFilePath());
            XSSFSheet sheet = wb.getSheet(UobConstants.UOB_SRC_SYS_SPEC);
            if (sheet == null)
                throw new EDAGPOIException("There is no \"File Spec\" Sheet in the Interface Specification");

            for (row = 0; row <= sheet.getLastRowNum(); row++) {
                if (row < 3) continue; // Blank and Header Lines
                else {
                    XSSFRow xssfRow = sheet.getRow(row);
                    if (xssfRow == null) continue;
                }
                if (!StringUtils.trimToEmpty(POIUtils.getCellContent(sheet, row,
                        getFileSpecFileDescriptionColNum(), String.class))
                        .equalsIgnoreCase(UobConstants.CONTROL_FILE_PATTERN) &&
                        !StringUtils.trimToEmpty(POIUtils.getCellContent(sheet, row,
                                getFileSpecFileTypeColNum(), String.class))
                                .equalsIgnoreCase(UobConstants.CONTROL_FILE_PATTERN)){

                    String fileName = POIUtils.getCellContent(sheet, row, getFileSpecFileNameColNum(), String.class);
                    if(fileName != null && !fileName.isEmpty()){
                        logger.debug(LOGGER_TAG + "Valid FileName: "+ fileName );
                        validFileNamesList.add( fileName );
                    }
                }
            }
        }
        catch (EDAGValidationException | EDAGPOIException e) {
            logger.error(LOGGER_TAG + Throwables.getStackTraceAsString(e));
            throw new EDAGException("Encountered Error while trying to parse \"File Spec\" from  "
                    + getInterfaceSpecFilePath() +" Workbook");
        }

        finally {

            try {
                wb.close();
            } catch (IOException e) {
                logger.error(LOGGER_TAG + Throwables.getStackTraceAsString(e));
            }
        }

        return validFileNamesList;
    }


    /**
     * Primary Method to generate HQL Vies
     * @param type the type of view it is eg. ns vs s
     * @throws Exception
     */
    private void generateHQLViews(String type, ArrayList<String> piiFields, String fileName)
            throws Exception {
        getApplicableDatabases(type, piiFields, fileName);
    }


    /**
     * Prepare the Output file path and name
     * @param type the option to replace
     * @return
     */

    private String getOutputfile(String type){
        resetPathsContext();
        logger.debug(LOGGER_TAG + "outfilename before replace: " + getOutfilename());
        setOutfilename(getOutfilename()
                .replaceAll("<source_system>", getSrcSystem())
                .replaceAll("<option>", type)
                .replaceAll("_<country_code>_", getCtryCdFromInterfaceFileName()));

        setCleanupFileName(getOutfilePath() + "/" + getCleanupFileName());
        setOutfilePath(getOutfilePath() +  "/" + getOutfilename().toLowerCase());
        logger.debug(LOGGER_TAG + "outfilepath after replace and concat: "+ getOutfilePath());

        return getOutfilePath();

    }

    /**
     * Method that parses the Interface Spec Filename for the current registration process and
     * returns the country code.
     *
     * eg. checks EDW_SWT_SWIFT_File-Format Specification_SG_VER0.18.xlsx
     * returns _sg_
     *
     * @return the normalized filename
     */

    private String getCtryCdFromInterfaceFileName(){
        String filename = getInterfaceSpecFileName() // Regex to remove punctuation marks, except, "." and "_"
                .replaceAll("[!?\\\\-]+", UobConstants.UNDERSCORE)
                .toLowerCase();
        String ctry = UobConstants.EMPTY;
        logger.debug(LOGGER_TAG + "Normalized InterfaceSpec Filename: " + filename);

        Pattern ctry_pattern = Pattern.compile(FILENAME_CTRY_REGEX);
        Matcher matcher = ctry_pattern.matcher(filename);

        if (matcher.find()) {
            logger.debug(LOGGER_TAG + "Found matching pattern ctry from InterfaceSpec filename:"+ matcher.group(0));
            logger.debug(LOGGER_TAG + "Extracting country from filename:"+ matcher.group(1));
            ctry = UobConstants.UNDERSCORE + matcher.group(1);

        }
        return ctry + UobConstants.UNDERSCORE;
    }


    /**
     * Overloaded Method: For non-sensitive views
     * Extracts the field specs from the Interface Spec Object
     *
     * @param piiFields the List of PII Fields that has been identified, these will be tagged with _NULL_
     * @param fileName the Filename (process), not the absolute filename
     * @return Hashmap of all the field details (name, number, description) and target hive table name
     */

    private Map<String, ArrayList<LSM_rowData>> extractFieldSpec(ArrayList<String> piiFields, String fileName){

        Map<String, Map<String, InterfaceSpecMap>> allFieldSpecs = getSpec().getSrcFieldSpec();

        // Key = fileName, Value = (FIELD_NM, value of FIELD_NM).
        Map<String, ArrayList<LSM_rowData>> field_spec_map = new HashMap<>();

        for (Map.Entry<String, Map<String, InterfaceSpecMap>> entry : allFieldSpecs.entrySet()) {

            if(fileName.equals(entry.getKey())){

                ArrayList<LSM_rowData> alist_LSM_rowDatas = new ArrayList<>();

                // get applicable countrys and load strategy
                Map<String, String> FSM_rowData = extractFileSpec(fileName);
                logger.debug(LOGGER_TAG + "Applicable countries (CSV) for "+ fileName + ":" + FSM_rowData.get(CTRY));
                List<String> applicable_ctrys = new ArrayList<>(Arrays.asList(FSM_rowData
                        .get(CTRY).split(UobConstants.COMMA)));

                logger.debug(LOGGER_TAG + "Applicable countries for " + fileName + ":" + applicable_ctrys);
                String load_strategy = FSM_rowData.get(UobConstants.LOAD_STRTGY);

                for (String ctry : applicable_ctrys ){

                    for (Map.Entry<String, InterfaceSpecMap> p : entry.getValue().entrySet()) {

                        InterfaceSpecMap ism = p.getValue();

                        String field_name = ism.get(UobConstants.FIELD_NM).toString();
                        String record_type = ism.get(UobConstants.RCRD_TYPE).toString();
                        String field_desc =  ism.get(UobConstants.FIELD_DESC).toString();

                        // Allow suppressing of some fields. The field names here should match the ones in excel file.
                        if( (isEnableSuppressHqlField()
                                && suppressFieldFromNameDesc(field_name, field_desc))
                                || record_type.equalsIgnoreCase(getFileNameRowTypeFieldValueHeader())
                                || record_type.equalsIgnoreCase(getFileNameRowTypeFieldValueFooter())  ) {
                            logger.debug(LOGGER_TAG + "Field Suppression is Enabled? : " + isEnableSuppressHqlField());
                            logger.debug(LOGGER_TAG + "Not including in views the following fields:"
                                    + field_name + ", its either SUPPRESSED or HEADER  or FOOTER");
                        }

                        else{

                            // Fetch the PSM (process Spec Map) row data for this FileName
                            Map<String, String> PSM_rowData = extractProcessSpec(fileName);
                            LSM_rowData lsm_rowData = new LSM_rowData();

                            lsm_rowData.setCTRY_CD(ctry);
                            lsm_rowData.setTBL_NM(PSM_rowData.get(UobConstants.TABLE_NAME));

                            // Add the tag for NS and S views, to identify which field is to be suppressed.
                            if(piiFields.contains(field_name)) lsm_rowData.setFLD_NM(NULL_TAG + field_name);
                            else lsm_rowData.setFLD_NM(field_name);

                            lsm_rowData.setFLD_NUM(ism.get(UobConstants.FIELD_NUM).toString());
                            lsm_rowData.setFLD_DESC(field_desc);
                            lsm_rowData.setTGT_APLY_TYPE_CD(load_strategy);

                            alist_LSM_rowDatas.add(lsm_rowData);
                        }

                    }
                }
                field_spec_map.put(entry.getKey(), alist_LSM_rowDatas);
            }

        }
        return field_spec_map;

    }


    /**
     * Method to check against the properties list file
     * and see if there's a mactch between the field name or field description
     *
     * This is done for cases, where Record Type may be present in Field Description.
     * @param field_name the field name
     * @param field_desc field description
     * @return if the field should be suppressed.
     */
    private boolean suppressFieldFromNameDesc(String field_name, String field_desc){
        return getSuppressedHqlFields().contains(field_name) || getSuppressedHqlFields().contains(field_desc);
    }



    /**
     * Overloaded Method: For sensitive views
     * Extracts the field specs from the Interface Spec Object
     * No special masking/tagging of fields are done.
     *
     * @param fileName the Filename (process), not the absolute filename
     * @return Hashmap of all the field details (name, number, description) and target hive table name
     */

    private Map<String, ArrayList<LSM_rowData>> extractFieldSpec(String fileName){

        Map<String, Map<String, InterfaceSpecMap>> allFieldSpecs = getSpec().getSrcFieldSpec();
        // Key = fileName, Value = (FIELD_NM, value of FIELD_NM).
        Map<String, ArrayList<LSM_rowData>> field_spec_map = new HashMap<>();

        for (Map.Entry<String, Map<String, InterfaceSpecMap>> entry : allFieldSpecs.entrySet()) {

            if(fileName.equals(entry.getKey())){
                ArrayList<LSM_rowData> alist_LSM_rowDatas = new ArrayList<>();

                // get applicable countrys and load strategy
                Map<String, String> FSM_rowData = extractFileSpec(fileName);
                logger.debug(LOGGER_TAG + "Applicable countries (CSV) for "+ fileName + ":" + FSM_rowData.get(CTRY));
                List<String> applicable_ctrys = new ArrayList<>(Arrays.asList(FSM_rowData
                        .get(CTRY).split(UobConstants.COMMA)));

                logger.debug(LOGGER_TAG + "Applicable countries for " + fileName + ":" + applicable_ctrys);
                String load_strategy = FSM_rowData.get(UobConstants.LOAD_STRTGY);


                for (String ctry : applicable_ctrys ){
                    for (Map.Entry<String, InterfaceSpecMap> p : entry.getValue().entrySet()) {
                        logger.debug(LOGGER_TAG + "extractFieldSpec innerloop: " + p.getKey());
                        InterfaceSpecMap ism = p.getValue();
                        String field_name = ism.get(UobConstants.FIELD_NM).toString();
                        String record_type = ism.get(UobConstants.RCRD_TYPE).toString();
                        String field_desc =  ism.get(UobConstants.FIELD_DESC).toString();

                        // Allow suppressing of some fields.
                        if( (isEnableSuppressHqlField()
                                && suppressFieldFromNameDesc(field_name, field_desc))
                                || record_type.equalsIgnoreCase(getFileNameRowTypeFieldValueHeader())
                                || record_type.equalsIgnoreCase(getFileNameRowTypeFieldValueFooter()) ) {
                            logger.debug(LOGGER_TAG + "Field Suppression is Enabled? : " + isEnableSuppressHqlField());
                            logger.debug(LOGGER_TAG + "Not including in views the following fields:"
                                    + field_name + ",its either SUPPRESSED or HEADER  or FOOTER");
                        }
                        else {

                            // Fetch the PSM (process Spec Map) row data for this FileName
                            Map<String, String> PSM_rowData = extractProcessSpec(fileName);
                            LSM_rowData lsm_rowData = new LSM_rowData();

                            lsm_rowData.setCTRY_CD(ctry);
                            lsm_rowData.setTBL_NM(PSM_rowData.get(UobConstants.TABLE_NAME));
                            lsm_rowData.setFLD_NM(field_name);
                            lsm_rowData.setFLD_NUM(ism.get(UobConstants.FIELD_NUM).toString());
                            lsm_rowData.setFLD_DESC(field_desc);
                            lsm_rowData.setTGT_APLY_TYPE_CD(load_strategy);

                            alist_LSM_rowDatas.add(lsm_rowData);
                        }
                    }
                }

                field_spec_map.put(entry.getKey(), alist_LSM_rowDatas);
            }

        }
        return field_spec_map;
    }


    /**
     * Extracts details from the File Spec Object,
     * eg. Country, Load Strategy (Delta, or Full Load)
     *
     * @param fileName the Filename (process) not absolute file path.
     * @return K,V
     */

    private Map<String, String> extractFileSpec(String fileName){

        Map<String, InterfaceSpecMap> allFileSpecs = getSpec().getSrcFileSpec();
        Map<String, String> file_spec_map = new HashMap<>();
        String csv_ctry = UobConstants.EMPTY_STRING;

        for (Map.Entry<String, InterfaceSpecMap> entry : allFileSpecs.entrySet()) {
            if(fileName.equals(entry.getKey())){
                for (Map.Entry<String, Object> p : entry.getValue().entrySet()) {

                    if (p.getKey().startsWith(CTRY + UobConstants.UNDERSCORE) && p.getValue()
                            .toString().equalsIgnoreCase(IS_TRUE))
                        csv_ctry += p.getKey().replace(CTRY +  UobConstants.UNDERSCORE, "")
                                + UobConstants.COMMA;

                    if(p.getKey().equals(UobConstants.LOAD_STRTGY)){
                        String load_type_strategy = p.getValue().toString();

                        if(p.getValue().toString().equalsIgnoreCase(UobConstants.APPEND_LOAD))
                            load_type_strategy = UobConstants.APPEND_LOAD_CD;
                        if(p.getValue().toString().equalsIgnoreCase(UobConstants.FULL_LOAD))
                            load_type_strategy = UobConstants.FULL_LOAD_CD;

                        file_spec_map.put(UobConstants.LOAD_STRTGY, load_type_strategy);

                    }
                }

                file_spec_map.put(CTRY, csv_ctry.replaceAll(",$", ""));

            }
        }

        return file_spec_map;

    }


    /**
     * Extracts details from the Process Spec Object
     * @param fileName fileName being processed.
     * @return the K,V
     */
    public Map<String, String> extractProcessSpec(String fileName){


        Map<String, Map<String, InterfaceSpecMap>> allProcessSpecs = getSpec().getProcessSpec();
        Map<String, String> PSM_rowData = new HashMap<>();

        // Read as, Key = fileName, Value = (TABLE_NAME, value of TABLE_NAME).
        for (Map.Entry<String, Map<String, InterfaceSpecMap>> entry : allProcessSpecs.entrySet()) {
            if(entry.getKey().equals(getInterfaceSpecFileName())){

                for (Map.Entry<String, InterfaceSpecMap> p : entry.getValue().entrySet()) {
                    InterfaceSpecMap ism = p.getValue();
                    logger.debug(LOGGER_TAG + "extractProcessSpec => fileName: "
                            + fileName +", FILE_NM: " + ism.get(UobConstants.FILE_NM));
                    if(fileName.equals(ism.get(UobConstants.FILE_NM).toString())){
                        PSM_rowData.put(UobConstants.TABLE_NAME, ism.get(UobConstants.TABLE_NAME).toString());
                        PSM_rowData.put(UobConstants.SRC_SYS_NM,ism.get(UobConstants.SRC_SYS_NM).toString());
                        PSM_rowData.put(UobConstants.PROCESS_ID,ism.get(UobConstants.PROCESS_ID).toString());
                        PSM_rowData.put(UobConstants.PROCESS_NAME,ism.get(UobConstants.PROCESS_NAME).toString());
                        PSM_rowData.put(UobConstants.PROCESS_GRP,ism.get(UobConstants.PROCESS_GRP).toString());
                    }

                }

            }

        }
        return PSM_rowData;
    }



    /**
     * Method to get the applicable database on which to apply the s and ns views.
     * @param fileName current filename being processed
     * @param type sensitive or non-sensitive views options
     * @param piiFields list of all pii identified fields
     * @throws Exception
     */
    private void getApplicableDatabases(String type,
                                        ArrayList<String> piiFields, String fileName ) throws Exception {

        Map<String, ArrayList<LSM_rowData>> results;

        // Do for S_VIEWS and NS_VIEWS
        if(type.equals(getNsViewsOption())) results = extractFieldSpec(piiFields, fileName);
        else results = extractFieldSpec(fileName);

        ConcurrentHashMap<String, SortedSet<Table>> ctryTableMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<Table, SortedMap<Integer, Field>> tableFieldMap = new ConcurrentHashMap<>();

        ArrayList<String> maskedFields = new ArrayList<>();

        for (Map.Entry<String, ArrayList<LSM_rowData>> entry : results.entrySet()) {
            logger.debug(LOGGER_TAG + "resultsSet => key: " + entry.getKey()+", value: " + entry.getValue());
            for (LSM_rowData rs: entry.getValue()){
                SortedMap<Integer, Field> ll = null;
                String tableName, ctrycode;
                ctrycode = rs.getCTRY_CD();
                tableName = rs.getTBL_NM();

                logger.debug(LOGGER_TAG + "[ctryCode, tableName] =>" + ctrycode + UobConstants.COMMA + tableName);

                String field_name = rs.getFLD_NM();

                if(field_name.contains(NULL_TAG)){
                    maskedFields.add(field_name.replaceAll(NULL_TAG, UobConstants.EMPTY));
                }


                Field field = new Field(rs.getFLD_NM());
                field.description = rs.getFLD_DESC();
                int colOrder = Integer.parseInt(rs.getFLD_NUM());
                String loadType = rs.getTGT_APLY_TYPE_CD();

                Table table = new Table(tableName, loadType);
                ll = tableFieldMap.getOrDefault(table, new TreeMap<>());
                ll.put(colOrder, field);

                tableFieldMap.put(table, ll);

                //!---- Reverted PII to Phase 1, sm186140 --->//

                if (!ctrycode.equals("ID") && !ctrycode.equals("MY") && !ctrycode.equals("TH") && !ctrycode.equals("CN")) {
                    SortedSet<Table> tblList0 = ctryTableMap.getOrDefault("so", new TreeSet<>());
                    tblList0.add(table);
                    ctryTableMap.put("so", tblList0);
                }
                else if (ctrycode.equals("MY")) {
                    SortedSet<Table> tblList1 = ctryTableMap.getOrDefault("my", new TreeSet<>());
                    tblList1.add(table);
                    ctryTableMap.put("my", tblList1);

                } else if (ctrycode.equals("TH")) {
                    SortedSet<Table> tblList2 = ctryTableMap.getOrDefault("th", new TreeSet<>());
                    tblList2.add(table);
                    ctryTableMap.put("th", tblList2);

                } else if (ctrycode.equals("CN")) {
                    SortedSet<Table> tblList3 = ctryTableMap.getOrDefault("cn", new TreeSet<>());
                    tblList3.add(table);
                    ctryTableMap.put("cn", tblList3);

                } else if (ctrycode.equals("ID")) {
                    SortedSet<Table> tblList4 = ctryTableMap.getOrDefault("id", new TreeSet<>());
                    tblList4.add(table);
                    ctryTableMap.put("id", tblList4);

                }
                if (!ctrycode.equals("ID")) {
                    SortedSet<Table> tblList5 = ctryTableMap.getOrDefault("gd", new TreeSet<>());
                    tblList5.add(table);
                    ctryTableMap.put("gd", tblList5);

                }
                // ---- End of PII Revert --->//


            }

        }

        prepareHQL(ctryTableMap, tableFieldMap, type, fileName, maskedFields);

    }

    /**
     * Method to prepare the map of all fields and description in HQL.
     * @param type sensitive views or non-sensitive views.
     * @throws Exception
     */
    private void prepareHQL(ConcurrentHashMap<String, SortedSet<Table>> ctryTableMap,
                            ConcurrentHashMap<Table, SortedMap<Integer, Field>> tableFieldMap,
                            String type,
                            String fileName,
                            ArrayList<String> maskedFields) throws Exception {
        logger.debug(LOGGER_TAG + "Starting "+ type +" HQL generation");


        logger.debug(LOGGER_TAG + "Fetching Velocity Template from: "+ getViewTemplateProperty());
        String template = VelocityUtils.getTemplateFromResource(getViewTemplateProperty(), true);

        Enumeration<String> keysMap = ctryTableMap.keys(); // map of country list
        logger.debug(LOGGER_TAG + "ctryTable Maps Lists (keys):" + keysMap);

        String sig = "-- " + fileName + UobConstants.SPACE + type
                + (type.equals(getNsViewsOption()) ?
                ", masked fields count:(" + maskedFields.size()+")  "
                        + String.join(UobConstants.COMMA
                        + UobConstants.SPACE, maskedFields)
                : UobConstants.EMPTY);

        while (keysMap.hasMoreElements()) {
            int piiFieldCounter = 0;
            String key = keysMap.nextElement();
            String ctrycode = UobConstants.EMPTY;

            SortedSet<Table> tables = ctryTableMap.get(key);


            logger.debug(LOGGER_TAG + "PII Reg Generator -  Group Name : " + key + " Views Count : " + tables.size());

            for (Table tbl : tables) {
                int counter = 0;

                String tablePrefix = UobConstants.EMPTY;

                Context ctx = new VelocityContext();


                String viewPrefix = UobConstants.ENVIRONMENT_STR_PARAM + UobConstants.UNDERSCORE + key
                        + UobConstants.UNDERSCORE + type.split(UobConstants.UNDERSCORE)[0] + "_v.";


                Collection<Field> fields = tableFieldMap.get(tbl).values();

                piiFieldCounter = piiFieldCounter + counter;
                ctx.put("fields", fields);
                logger.debug("fields: " +  fields);

                tablePrefix = UobConstants.ENVIRONMENT_STR_PARAM + UobConstants.UNDERSCORE + tbl.name.substring(4, 7);

                ctx.put("tableName", tablePrefix + "." + tbl.name);
                ctx.put("siteIdConditionKey", key);

                StringWriter strWriter;

                ctx.put("viewName", viewPrefix + tbl.name);
                strWriter = new StringWriter();
                VelocityUtils.evaluate(ctx, strWriter, ctx.get("viewName").toString(), template);

                StringBuilder HQLSegment = new StringBuilder();
                HQLSegment.append(isEnableSQLComments() ? sig.concat(UobConstants.NEWLINE) : UobConstants.EMPTY)
                        .append(strWriter.toString())
                        .append(isEnableSQLComments() ?  repeatString(2, UobConstants.NEWLINE) : UobConstants.EMPTY)
                        .append(UobConstants.NEWLINE);

                if(type.equals(getNsViewsOption())) getNsHQL().append(HQLSegment);
                else getsHQL().append(HQLSegment);

                if(isGenerateCleanupScript())
                    addDropStatement(tbl.name);

            }

            logger.debug(LOGGER_TAG + "PII Reg Generator - PII Fields Counter for Group : "
                    + key + " with counter " + piiFieldCounter);
        }
    }


    /**
     * Method to compile the list of Drop Statements.
     * @param tablename the tablename
     */

    private void addDropStatement( String tablename){
        List<String> view_types = Arrays.asList("ns_v.", "s_v.");

        List<String> db_list = Arrays.asList("gd", "so", "id", "th", "my", "cn");
        String prefix = "DROP VIEW IF EXISTS " + UobConstants.ENVIRONMENT_STR_PARAM + UobConstants.UNDERSCORE;

        for(String view_type : view_types) {
            for (String db : db_list) {
                drop_views.append(prefix)
                        .append(db)
                        .append(UobConstants.UNDERSCORE)
                        .append(view_type)
                        .append(tablename).append(UobConstants.SEMICOLON)
                        .append(UobConstants.NEWLINE);
            }
        }

    }

    /**
     * Utility to write the cleanupfile.
     */
    private void writeCleanupFile (){

        logger.debug("Writing CleanupFile at " +  getCleanupFileName());
        File file = new File(getCleanupFileName());
        try {
            FileUtils.writeStringToFile(file, drop_views.toString(), StandardCharsets.UTF_8, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Flush computed HQL views statement to file
     * @param HQL HQL views for every src sys.
     * @param type ns/s type
     * @throws Exception
     */

    private void writeHQLToFile(String HQL, String type) throws Exception {
        String sig = isEnableSQLComments() ? getBanner().replaceAll("<VIEW_TYPE>",type)
                .replaceAll("<SRC_SYS_CD>", getSrcSystem()).concat(UobConstants.NEWLINE) : UobConstants.EMPTY;

        String outputFile;
        outputFile = getOutputfile(type);
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(outputFile);
            bw = new BufferedWriter(fw);
            bw.write(sig.concat(HQL));
        } finally {
            if (bw != null) bw.close();
            if (fw != null) fw.close();
        }

        logger.debug(LOGGER_TAG + "PII Reg Generator HQLs written into " + outputFile);
    }


    /**
     * Read the initial PII fields from the FileSpecification Excel.
     * @param fileName file name
     * @param piiCol pii Column
     * @return list of PII enabled fields per filename
     * @throws Exception
     */

    private ArrayList<String> readPIIFieldsFromFileSpec(String fileName, String piiCol) throws Exception {

        logger.info(LOGGER_TAG + "Going to parse Sensitive Field Specifications for file: "
                + getInterfaceSpecFilePath());
        ArrayList<String> piiFields = new ArrayList<>();

        try {
            int col = Integer.parseInt(piiCol);
            // Assuming the actual excel column number is entered.
            col -= 1;

            try (OPCPackage pkg = OPCPackage.open(getInterfaceSpecFilePath())) {
                Workbook wb = WorkbookFactory.create(pkg);

                Sheet sheet = wb.getSheet(fileName);
                if (sheet == null) throw new EDAGException("Cannot find Excel Sheet by Name: " + fileName);
                else logger.info(LOGGER_TAG + "Processing Interface File Spec worksheet " + sheet.getSheetName());

                int piicounter = 0;

                Iterator<Row> rowIterator = sheet.rowIterator();

                while (rowIterator.hasNext()) {
                    Row xssfrow = rowIterator.next();
                    // Do check for Header, Footer, row type, PII column.
                    if(!isRowEmpty(xssfrow)
                            && !getFileNameRowTypeFieldValueHeader()
                            .equalsIgnoreCase((POIUtils.getCellContent(xssfrow, getFileNameRowTypeColNum()
                                    , String.class)))
                            && !getFileNameRowTypeFieldValueFooter()
                            .equalsIgnoreCase((POIUtils.getCellContent(xssfrow, getFileNameRowTypeColNum()
                                    , String.class)))
                            && getPiiFlag()
                            .equalsIgnoreCase(POIUtils.getCellContent(xssfrow, col, String.class))){

                        piiFields.add(POIUtils.getCellContent(xssfrow, getFileNameFieldNameColNum(), String.class));
                        piicounter++;

                    }
                }

                logger.info(LOGGER_TAG + "Total PII Fields found : fileName="+ fileName +", piiCounter=" + piicounter);
                logger.info(LOGGER_TAG + "List all PII Fields parsed : fileName="+ fileName +", piiFields=" + piiFields);

            }

        }
        catch (NumberFormatException e){
            logger.error(Throwables.getStackTraceAsString(e));
            throw new EDAGException("Cannot Parse PIICol, expected Integer", e.getCause());
        }

        return piiFields;
    }


    /**
     * Register the identified PII fields in the Metadata Table.
     * @param hiveTblNm Hive target table name for which the Views are to be generated.
     * @param fileName the File Name (process), not the absolute physical path
     * @param piiFields the List of PII fields read from the File Spec xlsx.
     */

    private void registerPIIFields(String hiveTblNm, String fileName, ArrayList<String> piiFields){

        try {
            PreparedStatement ps = getConnection().prepareStatement(getPsInsertSensitiveTemplate());
            for(String field : piiFields){
                ps.setInt(1, Integer.parseInt(getBatchNum()));
                ps.setString(2, getSrcSystem());
                ps.setString(3, hiveTblNm);
                ps.setString(4, fileName);
                ps.setString(5, field);

                ps.executeUpdate();
                getConnection().commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * Check if a row is empty.
     * @param row Row object.
     * @return true or false, if a row is empty respectively.
     */

    private boolean isRowEmpty(Row row) {
        if (row == null || row.getLastCellNum() <= 0) {
            logger.debug(LOGGER_TAG + "Encountered Empty row in Excel.");
            return true;
        }
        boolean empty = true;
        Iterator<Cell> cellIterator = row.cellIterator();
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            if (cell != null
                    && cell.getCellTypeEnum() != CellType.BLANK
                    && StringUtils.isNotBlank(cell.toString())) {
                empty = false;
                break;
            }
        }

        return empty;
    }


    /**
     * DB Connection Class
     * @throws EDAGException ClassNotFoundException, SQLException
     */
    private void setUpJDBCConnection() throws EDAGException {

        try {
            String driverName = PropertyLoader.getProperty(UobConstants.JDBC_DRIVER);
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            logger.error(Throwables.getStackTraceAsString(e));
            throw new EDAGException(LOGGER_TAG + "Could not find the JDBC driver class", e.getCause());
        }

        try {
            String driverConn = PropertyLoader.getProperty(UobConstants.JDBC_CONNECTION_URL);
            String userName = PropertyLoader.getProperty(UobConstants.JDBC_USERNAME);
            String password = new EncryptionUtil().decrypt(PropertyLoader.getProperty("JDBC.Password"));
            setConnection(DriverManager.getConnection(driverConn, userName, password));
        } catch (SQLException e) {
            logger.error(LOGGER_TAG + Throwables.getStackTraceAsString(e));
            throw new EDAGSQLException("Could not connect to SQL Server", e.getCause());

        }

    }


    /**
     * Quick utility to repeat the Strings
     * @param n number of times to repeat
     * @param s the string to repeat
     * @return the repeated string
     */
    public static String repeatString(int n, String s){
        return new String(new char[n]).replace("\0", s);
    }


    /**
     * Getters and Setters.
     */

    private static String getViewTemplateProperty() {
        return VIEW_TEMPLATE_PROPERTY;
    }

    private static String getPiiFlag() {
        return PII_FLAG;
    }

    private static void setPiiFlag(String piiFlag) {
        PII_FLAG = piiFlag;
    }

    private static String getBatchNum() {
        return BATCH_NUM;
    }

    private static void setBatchNum(String batchNum) {
        BATCH_NUM = batchNum;
    }

    private static String getCurrentTimestamp() {
        return CURRENT_TIMESTAMP;
    }

    private static String getSrcSystem() {
        return srcSystem;
    }

    private static void setSrcSystem(String srcSystem) {
        PIIRegistrationProcessor.srcSystem = srcSystem;
    }

    public InterfaceSpec getSpec() {
        return spec;
    }

    public void setSpec(InterfaceSpec spec) {
        this.spec = spec;
    }

    private String getInterfaceSpecFilePath() {
        return interfaceSpecFilePath;
    }

    private void setInterfaceSpecFilePath(String interfaceSpecFilePath) {
        this.interfaceSpecFilePath = interfaceSpecFilePath;
    }


    public String getInterfaceSpecFileName() {
        return interfaceSpecFileName;
    }

    public void setInterfaceSpecFileName(String interfaceSpecFileName) {
        this.interfaceSpecFileName = interfaceSpecFileName;
    }

    public String getProcessSpecFilePath() {
        return processSpecFilePath;
    }

    public void setProcessSpecFilePath(String processSpecFilePath) {
        this.processSpecFilePath = processSpecFilePath;
    }

    public String getProcessSpecFileName() {
        return processSpecFileName;
    }

    public void setProcessSpecFileName(String processSpecFileName) {
        this.processSpecFileName = processSpecFileName;
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void setConnection(Connection connection) {
        PIIRegistrationProcessor.connection = connection;
    }

    private String getOutfilePath() {
        return outfilePath;
    }

    private void setOutfilePath(String outfilePath) {
        this.outfilePath = outfilePath;
    }

    private String getOutfilename() {
        return outfilename;
    }

    private void setOutfilename(String outfilename) {
        this.outfilename = outfilename;
    }


    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    private static String getPsDeleteExistingPii() {
        return PS_DELETE_EXISTING_PII;
    }

    private static void setPsDeleteExistingPii(String psDeleteExistingPii) {
        PS_DELETE_EXISTING_PII = psDeleteExistingPii;
    }

    private static String getPsInsertSensitiveTemplate() {
        return PS_INSERT_SENSITIVE_TEMPLATE;
    }

    private static void setPsInsertSensitiveTemplate(String psInsertSensitiveTemplate) {
        PS_INSERT_SENSITIVE_TEMPLATE = psInsertSensitiveTemplate;
    }

    private static String getPsPrecheckExistsPii() {
        return PS_PRECHECK_EXISTS_PII;
    }

    private static void setPsPrecheckExistsPii(String psPrecheckExistsPii) {
        PS_PRECHECK_EXISTS_PII = psPrecheckExistsPii;
    }

    private static String getNsViewsOption() {
        return NS_VIEWS_OPTION;
    }

    private static String getsViewsOption() {
        return S_VIEWS_OPTION;
    }

    private static boolean isEnableSuppressHqlField() {
        return ENABLE_SUPPRESS_HQL_FIELD;
    }

    private static void setEnableSuppressHqlField(boolean enableSuppressHqlField) {
        ENABLE_SUPPRESS_HQL_FIELD = enableSuppressHqlField;
    }

    private static List<String> getSuppressedHqlFields() {
        return SUPPRESSED_HQL_FIELDS;
    }

    private static void setSuppressedHqlFields(List<String> suppressedHqlFields) {
        SUPPRESSED_HQL_FIELDS = suppressedHqlFields;
    }

    private static void setViewTemplateProperty(String viewTemplateProperty) {
        VIEW_TEMPLATE_PROPERTY = viewTemplateProperty;
    }


    private static String getPsDeleteExistingPiiWithoutFilename() {
        return PS_DELETE_EXISTING_PII_WITHOUT_FILENAME;
    }

    private static void setPsDeleteExistingPiiWithoutFilename(String psDeleteExistingPiiWithoutFilename) {
        PS_DELETE_EXISTING_PII_WITHOUT_FILENAME = psDeleteExistingPiiWithoutFilename;
    }

    private static int getFileSpecFileDescriptionColNum() {
        return FILE_SPEC_FILE_DESCRIPTION_COL_NUM;
    }

    private static int getFileSpecFileTypeColNum() {
        return FILE_SPEC_FILE_TYPE_COL_NUM;
    }

    private static int getFileSpecFileNameColNum() {
        return FILE_SPEC_FILE_NAME_COL_NUM;
    }

    private static String getFileNameRowTypeFieldValueHeader() {
        return FILE_NAME_ROW_TYPE_FIELD_VALUE_HEADER;
    }

    private static String getFileNameRowTypeFieldValueFooter() {
        return FILE_NAME_ROW_TYPE_FIELD_VALUE_FOOTER;
    }

    private static int getFileNameFieldNameColNum() {
        return FILE_NAME_FIELD_NAME_COL_NUM;
    }

    private static int getFileNameRowTypeColNum() {
        return FILE_NAME_ROW_TYPE_COL_NUM;
    }


    public static String getNullTag() {
        return NULL_TAG;
    }

    private static void setNullTag(String nullTag) {
        NULL_TAG = nullTag;
    }

    public static String getCTRY() {
        return CTRY;
    }

    private static void setCTRY(String CTRY) {
        PIIRegistrationProcessor.CTRY = CTRY;
    }

    public static String getIsTrue() {
        return IS_TRUE;
    }

    private static void setIsTrue(String isTrue) {
        IS_TRUE = isTrue;
    }

    private static void setNsViewsOption(String nsViewsOption) {
        NS_VIEWS_OPTION = nsViewsOption;
    }

    private static void setsViewsOption(String sViewsOption) {
        S_VIEWS_OPTION = sViewsOption;
    }

    private static void setCurrentTimestamp(String currentTimestamp) {
        CURRENT_TIMESTAMP = currentTimestamp;
    }

    private static void setFileSpecFileDescriptionColNum(int fileSpecFileDescriptionColNum) {
        FILE_SPEC_FILE_DESCRIPTION_COL_NUM = fileSpecFileDescriptionColNum;
    }

    private static void setFileSpecFileTypeColNum(int fileSpecFileTypeColNum) {
        FILE_SPEC_FILE_TYPE_COL_NUM = fileSpecFileTypeColNum;
    }

    private static void setFileSpecFileNameColNum(int fileSpecFileNameColNum) {
        FILE_SPEC_FILE_NAME_COL_NUM = fileSpecFileNameColNum;
    }

    private static void setFileNameRowTypeFieldValueHeader(String fileNameRowTypeFieldValueHeader) {
        FILE_NAME_ROW_TYPE_FIELD_VALUE_HEADER = fileNameRowTypeFieldValueHeader;
    }

    private static void setFileNameRowTypeFieldValueFooter(String fileNameRowTypeFieldValueFooter) {
        FILE_NAME_ROW_TYPE_FIELD_VALUE_FOOTER = fileNameRowTypeFieldValueFooter;
    }

    private static void setFileNameFieldNameColNum(int fileNameFieldNameColNum) {
        FILE_NAME_FIELD_NAME_COL_NUM = fileNameFieldNameColNum;
    }

    private static void setFileNameRowTypeColNum(int fileNameRowTypeColNum) {
        FILE_NAME_ROW_TYPE_COL_NUM = fileNameRowTypeColNum;
    }

    public static StringBuilder getNsHQL() {
        return nsHQL;
    }

    public static void setNsHQL(StringBuilder nsHQL) {
        PIIRegistrationProcessor.nsHQL = nsHQL;
    }

    public static StringBuilder getsHQL() {
        return sHQL;
    }

    public static void setsHQL(StringBuilder sHQL) {
        PIIRegistrationProcessor.sHQL = sHQL;
    }

    public static String getLoggerTag() {
        return LOGGER_TAG;
    }

    public void setLoggerTag() {
        setLoggerTag(UobConstants.EMPTY);
    }

    public void setLoggerTag(String fileName) {
        LOGGER_TAG = (fileName.isEmpty() ? "":"["+fileName+"]");
    }


    public boolean isEnableSQLComments() {
        return enableSQLComments;
    }

    public void setEnableSQLComments(boolean enableSQLComments) {
        this.enableSQLComments = enableSQLComments;
    }

    public void setEnableSQLComments(String enableSQLComments) {
        this.enableSQLComments = Boolean.parseBoolean(enableSQLComments);
    }

    public static String getFilenameCtryRegex() {
        return FILENAME_CTRY_REGEX;
    }

    public static void setFilenameCtryRegex(String filenameCtryRegex) {
        FILENAME_CTRY_REGEX = filenameCtryRegex;
    }

    public boolean isGenerateCleanupScript() {
        return generateCleanupScript;
    }

    public void setGenerateCleanupScript(boolean generateCleanupScript) {
        this.generateCleanupScript = generateCleanupScript;
    }

    public StringBuilder getDrop_views() {
        return drop_views;
    }

    public void setDrop_views(StringBuilder drop_views) {
        this.drop_views = drop_views;
    }

    public String getCleanupFileName() {
        return cleanupFileName;
    }

    public void setCleanupFileName(String cleanupFileName) {
        this.cleanupFileName = cleanupFileName;
    }

    public String getBanner(){
        String lines = repeatString(160, "-");

        return String.format("%s%s-- Generated PII <VIEW_TYPE> during registration " +
                        "for source system <SRC_SYS_CD> based on %s at %s%s%s%s"
                , lines
                , isEnableSQLComments() ?  UobConstants.NEWLINE : UobConstants.EMPTY
                , "\"" + getInterfaceSpecFileName() + "\""
                , new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())
                , isEnableSQLComments() ?  UobConstants.NEWLINE : UobConstants.EMPTY
                , lines
                , isEnableSQLComments() ?  repeatString(2, UobConstants.NEWLINE) : UobConstants.EMPTY);

    }

    @Override
    public String toString() {
        return "PIIRegistrationProcessor[" +
                "spec=" + spec +
                ", interfaceSpecFilePath='" + interfaceSpecFilePath + '\'' +
                ", outfilePath='" + outfilePath + '\'' +
                ", outfilename='" + outfilename + '\'' +
                ']';
    }



    /**
     * Table Class for storing the fields and target country specific Hive DB names
     */

    public class Table implements Comparable<Table> {
        private String name;
        private String loadType;

        private Table(String name, String loadType) {
            this.name = StringUtils.trimToEmpty(name);
            this.loadType = StringUtils.trimToEmpty(loadType);
        }

        public String getName() {
            return name;
        }

        public String getLoadType() {
            return loadType;
        }

        public boolean equals(Object obj) {
            boolean result = obj instanceof Table;

            if (result) {
                Table o  = (Table) obj;
                result = o.name.equals(this.name);
            }

            return result;
        }

        public int hashCode() {
            return this.name.hashCode();
        }

        @Override
        public int compareTo(Table o) {
            return o == null ? 1 : this.name.compareTo(o.name);
        }

        @Override
        public String toString() {
            return "PII Table [" +
                    "name='" + name + '\'' +
                    ", loadType='" + loadType + '\'' +
                    ']';
        }
    }


    /**
     * The Field class, used to store the field attributes.
     */
    public class Field {

        private String name;
        private String description;

        private Field(String name) {
            this.name = StringUtils.trimToEmpty(name);
        }

        public boolean isNull() {
            return name.contains(NULL_TAG);
        }

        public String getNormalizedName() {
            return com.uob.edag.utils.StringUtils
                    .normalizeForHive(name.replace(NULL_TAG, ""))
                    .toLowerCase();
        }

        public String getNormalizedDescription() {
            return com.uob.edag.utils.StringUtils.normalizeForHive(description, true);
        }


        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }


        @Override
        public String toString() {
            return "PII Field{" +
                    "name='" + name + '\'' +
                    ", description='" + description + '\'' +
                    '}';
        }
    }


    /**
     * Class to represent the prepared row from
     * Field Spec, File Spec and Process Spec
     *
     * Used to wrap and unfold the objects.
     */
    private class LSM_rowData {
        private String CTRY_CD;
        private String TBL_NM;
        private String FLD_NM;
        private String FLD_NUM;
        private String FLD_DESC;
        private String TGT_APLY_TYPE_CD;

        public String getCTRY_CD() {
            return CTRY_CD;
        }

        public void setCTRY_CD(String CTRY_CD) {
            this.CTRY_CD = CTRY_CD;
        }

        public String getTBL_NM() {
            return TBL_NM;
        }

        void setTBL_NM(String TBL_NM) {
            this.TBL_NM = TBL_NM;
        }

        public String getFLD_NM() {
            return FLD_NM;
        }

        public void setFLD_NM(String FLD_NM) {
            this.FLD_NM = FLD_NM;
        }

        public String getFLD_NUM() {
            return FLD_NUM;
        }

        public void setFLD_NUM(String FLD_NUM) {
            this.FLD_NUM = FLD_NUM;
        }

        public String getFLD_DESC() {
            return FLD_DESC;
        }

        public void setFLD_DESC(String FLD_DESC) {
            this.FLD_DESC = FLD_DESC;
        }

        public String getTGT_APLY_TYPE_CD() {
            return TGT_APLY_TYPE_CD;
        }

        public void setTGT_APLY_TYPE_CD(String TGT_APLY_TYPE_CD) {
            this.TGT_APLY_TYPE_CD = TGT_APLY_TYPE_CD;
        }

        @Override
        public String toString() {
            return "PII LSM_rowData{" +
                    "CTRY_CD='" + CTRY_CD + '\'' +
                    ", TBL_NM='" + TBL_NM + '\'' +
                    ", FLD_NM='" + FLD_NM + '\'' +
                    ", FLD_NUM='" + FLD_NUM + '\'' +
                    ", FLD_DESC='" + FLD_DESC + '\'' +
                    ", TGT_APLY_TYPE_CD='" + TGT_APLY_TYPE_CD + '\'' +
                    '}';
        }
    }

}
