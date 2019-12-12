package com.uob.edag.processor;

import com.google.common.base.Throwables;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.model.*;
import com.uob.edag.utils.BOMUtils;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.PropertyLoader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.time.StopWatch;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

import static com.uob.edag.constants.UobConstants.EMPTY_STRING;

/**
 * <h3>Overview</h3>
 * Class for processing the ingestion of SWIFT files.
 * <br><br><b>Swift File Format and Charsets</b>
 * <ul>
 *     <li>
 *         Swift Files are comma delimited files, and string enclosed in double
 *         quotes. Numeric Values are received as formatted numbers, eg. "345,550,123".
 *     </li>
 *
 *     <li>
 *         The files have BOM (Byte Order Mark) in the begininng for the non-standard encoding
 *          UTF-16LE (Little Endian). Additional generic class
 *
 *          Class included for handling BOM
 *          @see com.uob.edag.utils.BOMUtils
 *     </li>
 *
 *     <li>
 *         For SG, the default framework Charset UTF-8 is used.
 *         During registration, the property UTF-16LE_CHARSET=UTF-16LE is used to add
 *         UTF-16LE Charset to framework's known set of Charset.
 *     </li>
 *     <li>
 *        Do note as of this release adding the UTF-16LE as SG_CHARSET=UTF-16LE would break
 *        other existing SG files that use non-defined SG_CHARSET as UTF-8 framework default
 *        fallback.
 *     </li>
 *
 * </ul>
 *
 *
 * <b>Processing Logic</b>
 * <ul>
 *     <li><b>Pre-processing</b>
 *          <ul>
 *              <li>
 *                  Fetch a list of registered fields (columns) from the metadata
 *                  @see SwiftFileIngestionProcessor#getRegisteredFields()
 *                  @see com.uob.edag.dao.IngestionDao#getFieldNamePatterns(String)
 *                  @see com.uob.edag.dao.IngestionDao#getOrderedFieldNames(String)
 *              </li>
 *              <li>
 *                  Set a list for storing the relative column index when matching
 *                  between metadata and received file columns.
 *                  @see SwiftFileIngestionProcessor#headerColumnIndexReference
 *
 *                  Initialized with -1. Matching rows is overwritten with
 *                  respective index values.
 *                  @see SwiftFileIngestionProcessor#initializeHeaderColumnIndexReference()
 *              </li>
 *              <li>
 *                  Check the if there are files available in 'landing folder' that
 *                  match the metadata regex.
 *                  @see SwiftFileIngestionProcessor#checkPossibleFiles
 *
 *              </li>
 *          </ul>
 *     </li>
 *
 *     <li><b>Processing</b>
 *          <ul>
 *              <li>
 *                  For all detected files (files with names matching the metadata regex)
 *                  start the processes.
 *              </li>
 *              <li>
 *                  Backup the existing File Model Obj
 *                  @see SwiftFileIngestionProcessor#backupFileModel()
 *              </li>
 *              <li>
 *                  Move the current file from landing folder to archive folder
 *                  @see SwiftFileIngestionProcessor#moveExistingFile
 *              </li>
 *              <li>
 *                  Get the Charset from the Metadata. This will be used to parse the file.
 *              </li>
 *              <li>
 *                  Read from the file
 *                  @see SwiftFileIngestionProcessor#readFromFile(File, Charset)
 *                  <ul>
 *                      <li>
 *                          Parse the collected content from file body
 *                          @see SwiftFileIngestionProcessor#parseAndOrganizeLine
 *                      </li>
 *                      <li>
 *                          Try to detect the header
 *                          @see SwiftFileIngestionProcessor#detectHeaderRow(String)
 *                      </li>
 *                      <li>
 *                          Check if data rows are of expected length.
 *                          @see SwiftFileIngestionProcessor#isExpectedRowSize
 *                      </li>
 *                      <li>
 *                          If, data row size (columns) is less that
 *                          @see SwiftFileIngestionProcessor#expectedNoOfColumns
 *                          discard the row.
 *
 *                          If, data row size (columns) is more than or equal to
 *                          expected number of columns, re-arrange the rows to pick
 *                          known columns. Also, remove any duplicate columns.
 *
 *                          @see SwiftFileIngestionProcessor#rearrangeColumns(String)
 *                          @see SwiftFileIngestionProcessor#headerColumnIndexReference
 *                      </li>
 *
 *                  </ul>
 *              </li>
 *              <li>Write Modified File
 *                  <ul>
 *                      <li>
 *                          write the modified file with re-arranged columns and truncated
 *                          rows (if any). Filename is as per fileSpec, eg. UOB_report_Payments_Amount
 *                          @see SwiftFileIngestionProcessor#writeToFile(File, String, String)
 *                      </li>
 *                      <li>
 *                          Delete the existing original src file.
 *                          @see SwiftFileIngestionProcessor#removeFile()
 *                      </li>
 *                  </ul>
 *              </li>
 *              <li>
 *                  Run Regular ingestion
 *                  @see com.uob.edag.processor.FileIngestionProcessor#runFileIngestion(ProcessInstanceModel, ProcessModel, String, String, boolean, String)
 *              </li>
 *
 *              <li>
 *                  Post run cleanup includes resetting of the FileModel Objs.
 *                  @see SwiftFileIngestionProcessor#resetFileModel()
 *              </li>
 *          </ul>
 *
 *     </li>
 *
 *
 *
 * </ul>
 *
 * <b>Dynamic Columns Handling Logic</b>
 *
 *  <b>Targetted Cases</b>
 *      <ul>
 *          <li>Check if all the header columns are available in current file.</li>
 *          <li>Identify the correct Header Columns</li>
 *          <li>Re-order the columns correctly for informatica to pick up.</li>
 *          <li>Detect Duplicate Columns (Beyond TS Scope)</li>
 *      </ul>
 *
 *
 *  <b>Logic</b>
 *  <ol>
 *      <li>
 *          PRE_REQ: First get the edag_field_name_patterns.
 *          Lets, say edag_field_name_patterns returned Out of Order Header Columns (HC):
 *          eg. let's call this result, field_name_patterns <br>
 *          hc_2 = HC_2, hc2 <br>
 *          hc_1 = HC1, HC_1, hc_1 <br>
 *          hc_4 = HC_4 <br>
 *          hc_5 = HC_5, hc_5, hc5, HC5 <br>
 *          hc_3 = HC_3 <br>
 *      </li>
 *      <li>
 *          PRE_REQ: Create an array list col_metadata, and enter in order (RE-ARRANGE) the above result. <br>
 *          eg. [ [HC1, HC_1, hc_1], [HC_2, hc2] , [HC_3] , [HC_4]  , [HC_5, hc_5, hc5, HC5]  ]
 *      </li>
 *      <li>
 *          PRE_REQ: Now, split the received row
 *      </li>
 *      <li>
 *          _REQ_1 &amp; _2 &amp; 3__IDENTIFY HEADER COLUMN &amp;&amp; REORDER COLUMNS IN INGESTION FILE <br>
 *          For every col_file in row, compare with col_metadata, and update the headerColumnIndexReference.
 *      </li>
 *      <li>
 *          _REQ_4__DETECT DUPLICATES<br>
 *
 *          Maintain a separate List, alreadyFoundColumnMeta[]<br>
 *          Set, alreadyFoundColumnMeta[] as all '0' m where size = size of field_name_patterns.<br>
 *          If, found matching header in 4, set corresponding in alreadyFoundColumnMeta[] as 1<br>
 *          When iterating through Step 4, continue (skip) if index of alreadyFoundColumnMeta[] is already 1.<br>
 *      </li>
 *
 * <li>Technical Specification
 *     (see <a href="https://thinkbiganalytics.atlassian.net/browse/EDF-225">EDF-225</a>)</li>
 *  </ol>
 *
 *
 *
 * @author     Subhadip Mitra
 * @version     %I%, %G%
 * @since       1.0
 */

public class SwiftFileIngestionProcessor extends FileIngestionProcessor implements IngestionProcessor {

    private FileUtility fileUtils;
    private FileModel fileModel = null;
    private ProcessInstanceModel procInstanceModel = null;
    private String srcFilePath = "";
    private int expectedNoOfColumns = 0;
    private ArrayList<Integer> headerColumnIndexReference;
    private char CHAR_COMMA;
    private ProcessModel processModel;
    private int lineCount = 0; // Number of records
    private int failedCount = 0; // Number of records
    private String charset;
    private String FILE_LOGGER_TAG;
    private boolean doDataRowsNext = false;

    // Backup placeholders for File Model values from metadata
    private String metaSourceFileName;
    private String metaSourceDirectory;
    private String metaSourceArchivalDir;
    private ArrayList<ArrayList<String>> orderedFieldNames = new ArrayList<>();

    private static final String REGEX_DOUBLE_QUOTES_SPLIT = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
    private static final String REGEX_NORMALIZE_STRING = "[^A-Za-z0-9]+";

    private static boolean processMultipleFiles; // current framework does not support multiple files processing.

    // Benchmark specifics.
    private static boolean enableMetrics = false;
    private static final int MB = 1024*1024;
    private static final int ms = 1000 * 1000; // milliseconds
    private static Runtime runtime;
    private static StopWatch stopWatch;

    /**
     * This method is used to run the Ingestion Process for SWIFT File based source system.
     *
     * @param procInstanceModel The process Instance Model object
     * @param processModel      The Process Model object of the Ingestion Process
     * @param bizDate           The business date of the process
     * @param ctryCd            The country code of the process
     * @param forceRerun        Indicator to show if the Ingestion has to be force rerun from the start
     * @param forceFileName     Name of the file which has to force ingested
     * @throws EDAGException    when there is an error in the file Ingestion process
     */
    public void runFileIngestion(ProcessInstanceModel procInstanceModel, ProcessModel processModel, String bizDate, String ctryCd,
                                 boolean forceRerun, String forceFileName) throws EDAGException {

        setEnableMetrics(logger.isDebugEnabled() || logger.isTraceEnabled()); // Metrics are enabled to true if Debug/trace is enabled.





        // Setting up the benchmarks params.
        if(isEnableMetrics()) {
            setLocalRuntime(Runtime.getRuntime());
            setStopWatch(new StopWatch());
        }

        setProcessMultipleFiles(false); // current framework does not support multiple files processing.

        setFileUtils(FileUtilityFactory.getFileUtility(processModel));
        setProcessModel(processModel);
        setFileModel(processModel.getSrcInfo());
        setProcInstanceModel(procInstanceModel);
        setExpectedNoOfColumns(getFileModel().getSrcFieldInfo().size());
        setHeaderColumnIndexReference(new ArrayList<>(getExpectedNoOfColumns()));
        setCHAR_COMMA(PropertyLoader.getProperty("com.uob.edag.validation.InterfaceSpecHandler.FieldDelimiter.CSV").charAt(0));

        initializeHeaderColumnIndexReference();




        int fileCount = 0;

        // STEP 1: Check Files and iterate for all matched files in process Landing Dir.
        ArrayList<File> allDetectedFiles = checkPossibleFiles();

        // STEP 2: Get the list of all Fields from Metadata
        getRegisteredFields();

        logger.debug("Found total of "+ allDetectedFiles.size() +  " matching files for procID "+ this.processModel.getProcId());
        logger.debug(Arrays.toString(allDetectedFiles.toArray()));

        for(File actualSrcFile : allDetectedFiles){

            if(isEnableMetrics()){
                getStopWatch().reset();// reset the stopwatch for looped exec.
                getStopWatch().start();// Start the stop watch, init exec time.
            }

            ++fileCount;
            setFILE_LOGGER_TAG("["+ fileCount +"/"+ allDetectedFiles.size() +"]["+ actualSrcFile.getName() +"] ");

            logger.info(FILE_LOGGER_TAG + "Beginning Processing of File :" + actualSrcFile.getName());

            // STEP 3: Backup existing file model paths.
            backupFileModel();

            // EDF 236
            procInstanceModel = BaseProcessor.setFileSizeTime(actualSrcFile.getPath(), procInstanceModel); // EDF 236
            logger.info(String.format("Re-setting the file size for Excel file [%s]:%s", actualSrcFile, procInstanceModel.getSrcFileSizeBytes()));

            ingestDao.updateProcessLogFileSizeTime(procInstanceModel); // EDF 236
            logger.info("Updated SWT File Attributes = { " + "file size (bytes): " + procInstanceModel.getSrcFileSizeBytes()
                    + ", file arrival time: " + procInstanceModel.getSrcFileArrivalTime() + " }");
            // End of EDF 236


            // STEP 4:  Move the existing file.
            moveExistingFile(actualSrcFile);

            // STEP 5: Retrieve the charset and set
            CountryAttributes attrs = processModel.getCountryAttributesMap().get(procInstanceModel.getCountryCd());
            setCharset(attrs != null ? attrs.getCharset(true) : PropertyLoader
                    .getProperty(UobConstants.DEFAULT_CHARSET));

            logger.debug(FILE_LOGGER_TAG + "Current Charset resolved: " + getCharset());

            try {

                // STEP 6: Extract the Lines from File as a list
                readFromFile(new File(getSrcFilePath()), Charset.forName(getCharset()));

                // STEP 7 & 8:  see above method. Lines are read one at a time,
                //              processed and written for not blowing up the memory.

                // STEP 9: File is writen with name as per filespec eg. UOB_report_Payments_Amount.
                // Original File is archived as TIMESTAMP.gz in previous folder
                removeFile();

                // Keep a record of how many records were successfully identified and parsed, and how many failed.
                logger.debug(FILE_LOGGER_TAG + "Record Counts: SuccessFully Identified("
                        + lineCount +"), Failed("+ failedCount +")");

            } catch (IOException e) {
                logger.error(FILE_LOGGER_TAG + Throwables.getStackTraceAsString(e));
            }

            logger.info(FILE_LOGGER_TAG + "Number of source records: " + getLineCount());

            // STEP 10: Set number of records in file. Used for reconciliation.
            super.setNoOfRecords(getLineCount());


            if(isEnableMetrics()) { // link to debug/trace level.
                // Stop the stop watch.
                getStopWatch().stop();
                logger.debug(FILE_LOGGER_TAG + "Completed pre-processing in (millis): " + getStopWatch().getNanoTime() / ms);
                // Print the memory stats Pre-Ingestion.
                collectMemoryStatsSplit();
            }

            // STEP 11: Run regular file ingestion.
            super.runFileIngestion(procInstanceModel, processModel, bizDate, ctryCd, forceRerun, forceFileName);

            // STEP 12: Update the File Loading Log (EDAG_LOADED_FILE_LOG)
            updateFileLoadingLog(actualSrcFile.getName());

            // STEP 13: Reset the File Model to Metadata Defaults.
            resetFileModel();

            // Continue for other detected files in Landing Dir.
            logger.info(FILE_LOGGER_TAG +  "Finished Processing SWIFT File: " + actualSrcFile.getName());


            // process the loop only once, if multiple file processing is set to false.
            if(!isProcessMultipleFiles()) break;

        }
        // Print the memory stats - Post ingestion finalization.
        if(isEnableMetrics())collectMemoryStatsSplit(true);
    }



    /**
     * Initialize the List Row Index Reference with -1.
     * Use to re-arrange columns.
     */
    private void initializeHeaderColumnIndexReference(){
        IntStream.range(0, getExpectedNoOfColumns()).forEach(i -> getHeaderColumnIndexReference().add(-1));
    }

    /**
     * Method to fetch registered fields from Metadata
     * @throws EDAGMyBatisException
     */
    private void getRegisteredFields() throws EDAGMyBatisException {

        Map<String, List<String>> fieldNamePatterns;
        fieldNamePatterns = ingestDao.getFieldNamePatterns(getProcessModel().getProcId());
        logger.debug("All known field permutations: " + fieldNamePatterns);

        List<String> orderedFieldIds = ingestDao.getOrderedFieldNames(getProcessModel().getProcId());
        logger.debug( "Ordered field names: "+ orderedFieldIds );

        // Now, re-arrange the fieldNamePatterns
        for (String orderedFieldId : orderedFieldIds)
            getOrderedFieldNames().add(new ArrayList<>(fieldNamePatterns.get(orderedFieldId)));

        logger.debug("Updated Array Lists with all Field Name patterns Ordered: " + getOrderedFieldNames());
    }



    /**
     * Parses the File Data, and rearranges the column values.
     * @param line Data line
     * @return list of ordered and rearranged columns.
     */
    private String parseAndOrganizeLine (String line) {
        String modifiedRow = null;
        String transformedNumRow;
        if((isExpectedRowSize(line) != null)){
            if(doDataRowsNext){ // If this is not Header
                transformedNumRow = removeCommasFromNumbers(line);

                modifiedRow = rearrangeColumns(transformedNumRow); // re-arrange here

                if(modifiedRow != null) lineCount++;
                else {
                    failedCount++;
                    logger.debug(FILE_LOGGER_TAG + "[6] Discarding Row (Size not as expected post re-arrange)");
                    modifiedRow = null;
                } // where the modified row size is less than expected size. Last Check.
            }
            else if (detectHeaderRow(line)) {
                logger.debug(FILE_LOGGER_TAG + "Header Detected:" + line);
                logger.debug(FILE_LOGGER_TAG + "Dynamic Row Mapping Arr:" + getHeaderColumnIndexReference());
                doDataRowsNext = true; // next do data rows.
            }
        }
        return modifiedRow;
    }

    /**
     * Function to Update the File Loading Log in Metadata.
     * This is used to generate the Reporting (row count per file)
     * and also the Purging Scripts.
     *
     * @param current_file_name Current Source File that is being processed
     */

    private void updateFileLoadingLog(String current_file_name) {

        logger.debug(FILE_LOGGER_TAG + "Updating File Loading Log..");
        String tempFileNm = getProcInstanceModel().getFileNm();
        try {
            // Update procInstanceModel Obj.
            getProcInstanceModel().setTempFileNm(current_file_name);
            getProcInstanceModel().setTempFileRowCount(getLineCount());
            ingestDao.insertLoadedFileLog(getProcInstanceModel());

        }
        catch (EDAGMyBatisException e) {
            logger.error(FILE_LOGGER_TAG + Throwables.getStackTraceAsString(e));

        }
        finally {
            // Reset to default values.
            getProcInstanceModel().setTempFileNm(tempFileNm);
            getProcInstanceModel().setTempFileRowCount(0); // redundant
        }

    }
    /**
     * Function to check all the possible files in the landing directory, that match the filename regex
     * defined in the EDAG_PROC_PARAMS.
     * @return a File Obj list of all matching files.
     * @throws EDAGMyBatisException
     */

    private ArrayList<File> checkPossibleFiles() throws EDAGMyBatisException {

        List<ProcessParam> procParams  = ingestDao.retrieveLoadParams(getProcessModel().getProcId());
        String SEQ_FILENAME = "FILE_NAME";
        String landingDir = Paths.get(getFileModel().getSourceDirectory()).getParent().toString();
        File[] testFiles;
        File dir = new File(landingDir);
        ArrayList<File> matchingFiles = new ArrayList<>();

        for(ProcessParam param : procParams)
            if (param.getParamName().contains(SEQ_FILENAME)) { // Check the key to match, eg. FILE_NAME_MONTHLY, FILE_NAME_YEARLY.
                testFiles = dir.listFiles((pathname) -> pathname.isFile() && pathname.getName().matches(param.getParamValue()));
                if (testFiles != null) matchingFiles.addAll(Arrays.asList(testFiles));
            }

        return matchingFiles;
    }


    /**
     * Utility function to move the existing (original file).
     * The new transformed file with the same name has to be created.
     * The old file is archived for record purposes.
     *
     * NB: the source file is of different name than the one in metadata
     *
     * @throws EDAGException IOException
     */

    private void moveExistingFile(File actualSrcFile) throws EDAGException {
        String archiveFilePath = String.format("%s%s.%s", getFileModel().getSourceArchivalDir()
                        .substring(0, getFileModel().getSourceArchivalDir().indexOf(getFileModel().getSourceFileName())),
                actualSrcFile.getName(), UobConstants.BIZ_DATE_PARAM);

        // Overwrite the Meta File Path.
        getFileModel().setSourceArchivalDir(archiveFilePath);
        archiveFilePath = archiveFilePath.replaceAll(UobConstants.BIZ_DATE_PARAM, getProcInstanceModel().getBizDate());

        // Update the fileModel Obj with the current actual file details.
        updateFileModel(actualSrcFile);

        logger.info(FILE_LOGGER_TAG + String.format("Archiving the Swift file %s", getFileModel().getSourceFileName()));
        logger.info(FILE_LOGGER_TAG + "archiveFilePath: " + archiveFilePath);

        getFileUtils().archiveFile(getFileModel().getSourceDirectory(), archiveFilePath, false);
    }


    /**
     * Function to update the fileModel with the current actual File path, etc.
     * NB: the actual src file is of a different name than the registered file name from filespec.
     * @param actualSrcFile the current actual src file.
     */

    private void updateFileModel(File actualSrcFile){

        // Change the Source Directory now with the actual filename
        getFileModel().setSourceFileName(actualSrcFile.getName());
        getFileModel().setSourceDirectory(actualSrcFile.getAbsolutePath());

        // Update the srcFilePath from the actual file path.
        setSrcFilePath(getFileModel().getSourceDirectory());
    }


    /**
     * On every looped run of file being processed reset
     * the File Model values to DEFAULT metadata values
     */
    private void resetFileModel(){

        // revert the Source Directory to metadata name
        getFileModel().setSourceFileName(getMetaSourceFileName());
        getFileModel().setSourceDirectory(getMetaSourceDirectory());

        // revert actual file path.
        setSrcFilePath(getFileModel().getSourceDirectory());
        getFileModel().setSourceArchivalDir(getMetaSourceArchivalDir());

    }


    /**
     * Backup the DEFAULT File Model metadata value
     */
    private void backupFileModel(){

        // Backup the Meta File Model Values, used to reset.
        setMetaSourceArchivalDir(getFileModel().getSourceArchivalDir());
        setMetaSourceDirectory(getFileModel().getSourceDirectory());
        setMetaSourceFileName(getFileModel().getSourceFileName());
    }


    /**
     * Simple file writer
     * @param file dest file
     * @param charset charset of destination, default, UTF-8
     * @param line the data to be flushed to file.
     * @throws IOException
     */
    private void writeToFile(File file, String charset, String line) throws IOException {
        FileUtils.writeStringToFile(file, line, charset, true);
    }

    /**
     * Read from File. Removes the BOM.
     * @param file the actual source file
     * @param charset the charset fetched from metadata
     * @return
     * @throws IOException
     */
    private void readFromFile(File file, Charset charset) throws IOException {
        InputStream in = new FileInputStream(file);
        String line, modifiedLine;
        boolean firstLine = true; // Add \n to all lines except the first line

        // Delete File. Force OS to flush.
        FileUtils.deleteQuietly(FileUtils.getFile(getMetaSourceDirectory()));

        // Get rid of the nagging BOM bytes.
        InputStream cleanStream = new BOMUtils(in).skipBOM();
        LineIterator it = IOUtils.lineIterator(cleanStream, charset);

        try {
            while (it.hasNext()) {
                line = it.nextLine();

                // STEP 7: Parse and re-arrange out of order columns
                modifiedLine = parseAndOrganizeLine(line);

                if(modifiedLine != null){

                    // Add new line to all lines except first line.
                    if(!firstLine) modifiedLine = UobConstants.NEWLINE.concat(modifiedLine);
                    else firstLine = false;

                    // STEP 8: Write to file the ordered columns. No Header, Columns rearranged
                    writeToFile(new File(getMetaSourceDirectory())
                            , PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET)
                            , modifiedLine); // parse and re-organize line.
                }
            }
        } finally {
            LineIterator.closeQuietly(it); // this is deprecated in v2.6 of commons-io.
        }


    }

    /**
     * Method that removes the Src File
     * The Src File is already backed up as TIMESTAMP.gz
     * @link moveExistingFile()
     */

    private void removeFile() {
        File fileToDelete = FileUtils.getFile(getSrcFilePath());
        if(FileUtils.deleteQuietly(fileToDelete)){
            logger.debug(FILE_LOGGER_TAG + "Deleted existing file (original file previously archived):"
                    + getSrcFilePath());

            // reset the file model
            resetFileModel();
        }
        else
            logger.debug(FILE_LOGGER_TAG + "Failed to overwrite file:" + getSrcFilePath() );
    }


    /**
     * Simple func to split row by CSV.
     * @param row csv file row.
     * @return
     */
    private List<String> splitRow(String row){
        return new ArrayList<>(Arrays.asList(row.split(REGEX_DOUBLE_QUOTES_SPLIT)));
    }



    /**
     * ArrayList extension that enables adding elements to indices
     * without throwing error. Fills intermediary indices with NULLs.
     * @param <E> Object Type
     */
    public class ArrayListElastic<E> extends ArrayList<E>{
        @Override
        public void add(int index, E element){
            if(index >= 0 && index <= size()){
                super.add(index, element);
                return;
            }
            int insertNulls = index - size();
            for(int i = 0; i < insertNulls; i++) super.add(null);
            super.add(element);
        }
    }


    /**
     * Rudimentary method to re-arrange rows.
     * Still O(n)
     * @param row sanitized row, with numeric commas stripped off.
     * @return
     */
    private String rearrangeColumns(String row){
        ArrayListElastic<String> new_row_list = new ArrayListElastic<>();
        int target_column;
        String updated_row = UobConstants.EMPTY_STRING;
        ArrayList<String> row_columns_list = new ArrayList<>(splitRow(row));

        for(int i = 0; i < row_columns_list.size(); i++){
            target_column = getHeaderColumnIndexReference().get(i);
            if(target_column != -1) new_row_list.add(target_column, row_columns_list.get(i)); // only if its matched
        }
        logger.debug(FILE_LOGGER_TAG + "[5] Rearranged Row:" + new_row_list);

        if(new_row_list.size() == getExpectedNoOfColumns()) {
            for (int j = 0; j < new_row_list.size(); j++) {
                updated_row += new_row_list.get(j);
                // dont add last comma
                if (j < new_row_list.size() - 1) updated_row += UobConstants.COMMA;
            }

            return updated_row;
        }
        return null;

    }

    /**
     * Utility Function to strip all punctuation
     * @param str string to normalize
     * @return
     */
    private String normalizeString(String str){
        return str.replaceAll(REGEX_NORMALIZE_STRING, "").toLowerCase();
    }

    /**
     * Function to detect header row.
     * Why write separately?
     * Because this func is optimized.
     * Time Complexity of this is O(m+n) where m and n are the size of the respective lists.
     *
     * @param row csv file row.
     * @return
     */
    private boolean detectHeaderRow(String row){

        logger.debug(FILE_LOGGER_TAG + "Current row received for Header Test:" + row);
        boolean detected = false;
        int total_col_found = 0;
        List <String> row_list = splitRow(row);

        ArrayList<String> col_file = new ArrayList<>(row_list);
        logger.debug(FILE_LOGGER_TAG + "Array list split row for Header Test: " + col_file);

        for(int i = 0; i < col_file.size(); i++)
            // if the ith - 'col_file' is found in 'orderedFieldNames', assign, the headerColumnIndexReference
            for (int j = 0; j < getOrderedFieldNames().size(); j++)
                if (!getHeaderColumnIndexReference().contains(j)
                        && getOrderedFieldNames().get(j)
                        .contains(col_file.get(i).toLowerCase().replaceAll("\"", ""))) {

                    logger.debug(FILE_LOGGER_TAG + "Matched Col: " + col_file.get(i).toLowerCase().replaceAll("\"", ""));
                    getHeaderColumnIndexReference().add(i, j); // update the index reference, will use this to re-arrange rows.

                    logger.debug(FILE_LOGGER_TAG + "Updated value at headerColumnIndexReference: (" + i + "," + j + ")");
                    total_col_found++;

                    break; // get out of the loop
                }

        logger.debug(FILE_LOGGER_TAG + "Row Index reference:" + getHeaderColumnIndexReference());

        if(total_col_found == getOrderedFieldNames().size()) {
            detected = true;
            logger.info(FILE_LOGGER_TAG + "Found an exact match or subset of known header");
        }

        return detected;
    }



    /**
     * Utility function that checks if a given row is of Expected size
     * @return truncated row of expected size, or null if row size received is less than expected size
     *
     */

    private String isExpectedRowSize(String row){
        // Split the row as a list. Only split the comma's outside the double quotes
        List<String> rowList = splitRow(row);

        logger.debug(FILE_LOGGER_TAG + "[1] Expected number of columns:" + getExpectedNoOfColumns());
        logger.debug(FILE_LOGGER_TAG + "[2] Current size rowList: "+ rowList.size());

        // Get the difference of the sizes
        int diff = rowList.size() - getExpectedNoOfColumns();

        // when received row is less than expected row, discard
        return diff < 0 ? null : row;
    }



    /**
     * An utility function that removes that given a comma separated String in quotes
     * with formatted numbers, eg. "mike likes","123,4567", "oscar",
     * will return "mike likes","1234567", "oscar"
     * where the formatted numbers have their comma removed
     * @param str the input string row to modify
     * @return modified string
     */

    private String removeCommasFromNumbers(String str){

        logger.debug(FILE_LOGGER_TAG + "[3] Original Row String:" + str);
        boolean in_quotes = false;
        StringBuilder transformedStr = new StringBuilder(UobConstants.EMPTY_STRING);

        for(int i=0; i<str.length(); ++i) {
            char c = str.charAt(i);

            //check if this is a start or end of contained string
            if (c=='"') in_quotes = !in_quotes;

            // If its a comma and in quotes
            if (c==CHAR_COMMA && in_quotes) {
                // Check if its a digit.
                if(Character.isDigit(c)) transformedStr.append(EMPTY_STRING); //remove the comma
            }
            else transformedStr.append(str.charAt(i));// concatenate the chars
        }
        logger.debug(FILE_LOGGER_TAG + "[4] Transformed Row String (stripped numbers) : " + transformedStr);
        return transformedStr.toString();
    }


    /**
     * Small Utility to print mem stats for each execution.
     */
    private void collectMemoryStatsSplit() {
        collectMemoryStatsSplit(false);
    }

    /**
     * Small Utility to print mem stats for each execution.
     */
    private void collectMemoryStatsSplit(boolean suppressTag){

        //used memory
        logger.debug((suppressTag? "All Steps - ":FILE_LOGGER_TAG) + "Used Memory (MB):" + (getLocalRuntime().totalMemory() - getLocalRuntime().freeMemory()) / MB);

        //free memory
        logger.debug((suppressTag? "All Steps - ":FILE_LOGGER_TAG) + "Free Memory (MB):" + getLocalRuntime().freeMemory() / MB);

        //total available memory
        logger.debug((suppressTag? "All Steps - ":FILE_LOGGER_TAG) + "Total Memory (MB):" + getLocalRuntime().totalMemory() / MB);

        //Maximum available memory
        logger.debug((suppressTag? "All Steps - ":FILE_LOGGER_TAG) + "Max Memory (MB):" + getLocalRuntime().maxMemory() / MB);
    }


    // Getters and Setters

    public FileUtility getFileUtils() {
        return fileUtils;
    }

    public void setFileUtils(FileUtility fileUtils) {
        this.fileUtils = fileUtils;
    }

    public FileModel getFileModel() {
        return fileModel;
    }

    public void setFileModel(FileModel fileModel) {
        this.fileModel = fileModel;
    }

    public ProcessInstanceModel getProcInstanceModel() {
        return procInstanceModel;
    }

    public void setProcInstanceModel(ProcessInstanceModel procInstanceModel) {
        this.procInstanceModel = procInstanceModel;
    }

    public String getSrcFilePath() {
        return srcFilePath;
    }

    public void setSrcFilePath(String srcFilePath) {
        this.srcFilePath = srcFilePath;
    }

    public int getExpectedNoOfColumns() {
        return expectedNoOfColumns;
    }

    public void setExpectedNoOfColumns(int expectedNoOfColumns) {
        this.expectedNoOfColumns = expectedNoOfColumns;
    }

    public ArrayList<Integer> getHeaderColumnIndexReference() {
        return headerColumnIndexReference;
    }

    public void setHeaderColumnIndexReference(ArrayList<Integer> headerColumnIndexReference) {
        this.headerColumnIndexReference = headerColumnIndexReference;
    }

    public char getCHAR_COMMA() {
        return CHAR_COMMA;
    }

    public void setCHAR_COMMA(char CHAR_COMMA) {
        this.CHAR_COMMA = CHAR_COMMA;
    }

    public ProcessModel getProcessModel() {
        return processModel;
    }

    public void setProcessModel(ProcessModel processModel) {
        this.processModel = processModel;
    }

    public int getLineCount() {
        return lineCount;
    }

    public void setLineCount(int lineCount) {
        this.lineCount = lineCount;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getFILE_LOGGER_TAG() {
        return FILE_LOGGER_TAG;
    }

    public void setFILE_LOGGER_TAG(String FILE_LOGGER_TAG) {
        this.FILE_LOGGER_TAG = FILE_LOGGER_TAG;
    }

    public String getMetaSourceFileName() {
        return metaSourceFileName;
    }

    public void setMetaSourceFileName(String metaSourceFileName) {
        this.metaSourceFileName = metaSourceFileName;
    }

    public String getMetaSourceDirectory() {
        return metaSourceDirectory;
    }

    public void setMetaSourceDirectory(String metaSourceDirectory) {
        this.metaSourceDirectory = metaSourceDirectory;
    }

    public String getMetaSourceArchivalDir() {
        return metaSourceArchivalDir;
    }

    public void setMetaSourceArchivalDir(String metaSourceArchivalDir) {
        this.metaSourceArchivalDir = metaSourceArchivalDir;
    }

    public ArrayList<ArrayList<String>> getOrderedFieldNames() {
        return orderedFieldNames;
    }

    public void setOrderedFieldNames(ArrayList<ArrayList<String>> orderedFieldNames) {
        this.orderedFieldNames = orderedFieldNames;
    }

    public boolean isDoDataRowsNext() {
        return doDataRowsNext;
    }

    public void setDoDataRowsNext(boolean doDataRowsNext) {
        this.doDataRowsNext = doDataRowsNext;
    }

    public static String getRegexDoubleQuotesSplit() {
        return REGEX_DOUBLE_QUOTES_SPLIT;
    }

    public static String getRegexNormalizeString() {
        return REGEX_NORMALIZE_STRING;
    }

    public static int getMB() {
        return MB;
    }

    public static Runtime getLocalRuntime() {
        return runtime;
    }

    public static void setLocalRuntime(Runtime runtime) {
        SwiftFileIngestionProcessor.runtime = runtime;
    }

    public static StopWatch getStopWatch() {
        return stopWatch;
    }

    public static void setStopWatch(StopWatch stopWatch) {
        SwiftFileIngestionProcessor.stopWatch = stopWatch;
    }

    public static boolean isProcessMultipleFiles() {
        return processMultipleFiles;
    }

    public static void setProcessMultipleFiles(boolean processMultipleFiles) {
        SwiftFileIngestionProcessor.processMultipleFiles = processMultipleFiles;
    }

    public static boolean isEnableMetrics() {
        return enableMetrics;
    }

    public static void setEnableMetrics(boolean enableMetrics) {
        SwiftFileIngestionProcessor.enableMetrics = enableMetrics;
    }

    @Override
    public String toString() {
        return "SwiftFileIngestionProcessor[" +
                "fileUtils=" + fileUtils +
                ", fileModel=" + fileModel +
                ", procInstanceModel=" + procInstanceModel +
                ", srcFilePath='" + srcFilePath + '\'' +
                ", expectedNoOfColumns=" + expectedNoOfColumns +
                ", headerColumnIndexReference=" + headerColumnIndexReference +
                ", CHAR_COMMA=" + CHAR_COMMA +
                ", processModel=" + processModel +
                ", lineCount=" + lineCount +
                ", charset='" + charset + '\'' +
                ", FILE_LOGGER_TAG='" + FILE_LOGGER_TAG + '\'' +
                ", metaSourceFileName='" + metaSourceFileName + '\'' +
                ", metaSourceDirectory='" + metaSourceDirectory + '\'' +
                ", metaSourceArchivalDir='" + metaSourceArchivalDir + '\'' +
                ", orderedFieldNames=" + orderedFieldNames +
                ']';
    }



}
