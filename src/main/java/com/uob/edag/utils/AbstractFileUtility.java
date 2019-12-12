package com.uob.edag.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;

public abstract class AbstractFileUtility implements FileUtility {
	
	protected Logger logger = Logger.getLogger(getClass());
	
	public boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel,
                                          HadoopModel hadoopModel, String charsetName, boolean isSkipErrRecordsDisabled, int linesToValidate) throws EDAGException {
		return removeControlCharacter(fileName, outputFile, sourceInfo, controlModel, hadoopModel, charsetName, isSkipErrRecordsDisabled, null, linesToValidate);
    }
	
	public boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel,
                                          HadoopModel hadoopModel, String charsetName, boolean isSkipErrRecordsDisabled, Map<String, String> charReplacementMap) throws EDAGException {
		return removeControlCharacter(fileName, outputFile, sourceInfo, controlModel, hadoopModel, charsetName, isSkipErrRecordsDisabled, charReplacementMap, -1);
    }
	
	protected abstract boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel, HadoopModel hadoopModel,
                                                      String charsetName, boolean isSkipErrRecordsDisabled, Map<String, String> charReplacementMap, int i) throws EDAGException;

	/**
   * This method is used to read a particular number of lines in a file.
   * @param is The Input Stream for the file to be read
   * @param numLines The number of lines to be read from the file
   * @return a List of String with the contents in the file
   * @throws Exception when there is an error reading the lines of the file.
   */
  protected List<String> readNLines(InputStream is, int numLines, String charsetName) throws EDAGIOException {
    List<String> linesList = new ArrayList<String>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName(charsetName)))) {
      String line = null;
      int lineNum = 0;
      while ((line = br.readLine()) != null) {
        linesList.add(line);
        if (lineNum++ >= numLines) {
          break;
        }
      }

      logger.debug(linesList.size() + " read from input stream");
      return linesList;
    } catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_READ_INPUT_STREAM, e.getMessage());
		} 
  }
  
  /** This method is used to read file ignoring the SUB character in last line.
   * @param is Input Stream object for the file to be read
   * @return a List of String with the lines in the file
   * @throws Exception when there is an error reading the file
   */
  protected List<String> readFile(InputStream is, String charsetName) throws EDAGIOException {
    List<String> linesList = new ArrayList<String>();
    boolean closingReader = false;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is, Charset.forName(charsetName)))) {
      String line = null;
      while ((line = br.readLine()) != null) {
        linesList.add(line);
      }
      
      int len = linesList.size();
      if (!linesList.isEmpty() && linesList.get(len - 1) != null && 
      		linesList.get(len - 1).startsWith(UobConstants.SUBSTITUTE_CHAR)) {
        logger.debug("Last line is SUB character; Removing the line");
        linesList.remove(len - 1);
      }

      logger.debug(linesList.size() + " read from input stream");
      closingReader = true;
    } catch (IOException e) {
    	if (closingReader) {
    		logger.warn("Cannot close input stream reader: " + e.getMessage());
    	} else {
    		throw new EDAGIOException(EDAGIOException.CANNOT_READ_INPUT_STREAM, e.getMessage());
    	}
    } 
    
    return linesList;
  }
  
  /** This method is used to read the first or last line of a file using the indicator.
   * @param fileName the file to be read
   * @param lineInd The Line Indicator - First or Last
   * @param charsetName charset name
   * @return a String with the content of the requested line
   * @throws Exception when there is an error reading the file
   */
  public String readLineByIndicator(String fileName, String lineInd, String charsetName) throws EDAGIOException {
  	Charset charset = Charset.forName(charsetName);
  	
    String line = null;
    if (lineInd.equalsIgnoreCase(UobConstants.FIRST)) {
      try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), charset))) {
      	line = br.readLine();
      	logger.debug("Fist line read is " + line);
      } catch (IOException e) {
      	throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, fileName, e.getMessage());
      }
    } else if (lineInd.equalsIgnoreCase(UobConstants.LAST)) {
      try (ReversedLinesFileReader rfr = new ReversedLinesFileReader(new File(fileName), charset)) {
        line = rfr.readLine();
        if (line != null && line.startsWith(UobConstants.SUBSTITUTE_CHAR)) {
          logger.debug("Last line is SUB character; Reading the previous line");
          line = rfr.readLine();
        }
        logger.debug("Last line read is " + line);
      } catch (IOException e) {
      	throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, fileName, e.getMessage());
      }
    }  

    return line;
  }
  
  protected File writeToTempFile(InputStream is) throws EDAGIOException {
  	File file;
  	String now = Long.toString(System.currentTimeMillis());
		try {
			file = File.createTempFile(getClass().getName(), now);
		} catch (IOException e1) {
			throw new EDAGIOException(EDAGIOException.CANNOT_CREATE_TEMP_FILE, getClass().getName(), now, e1.getMessage());
		}
  	file.deleteOnExit();
  	logger.debug("Temp file " + file.getPath() + " created, delete on exit enabled");
  	
  	try (FileOutputStream out = new FileOutputStream(file)) { 
    	IOUtils.copy(is, out);
    	logger.debug("Input stream content copied to " + file.getPath());
  	} catch (FileNotFoundException e) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, file.getPath(), e.getMessage());
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_COPY_STREAM_CONTENT, e.getMessage());
		} 
    
    return file;
  }
  
  protected void executeCommand(String command) throws EDAGIOException {
    String dir = PropertyLoader.getProperty(UobConstants.SCRIPT_DIR);

    logger.debug("Going to execute command: " + command);
    Process proc;
    try {
	    proc = Runtime.getRuntime().exec(command, null, new File(dir));
	    proc.waitFor();
    } catch (IOException | InterruptedException e) {
    	throw new EDAGIOException(EDAGIOException.CANNOT_EXEC_CMD, command, e.getMessage());
    }
	
    int exitVal = proc.exitValue();
    try {
	    String output = IOUtils.toString(proc.getInputStream(), Charsets.UTF_8);
	
	    if (exitVal != 0) {
	      String errorOutput = IOUtils.toString(proc.getErrorStream(), Charsets.UTF_8);
	
	      String allOutput = "";
	      if (StringUtils.isNotEmpty(output)) {
	        allOutput = allOutput + "STDOUT: " + output + ";";
	      }
	      
	      if (StringUtils.isNotEmpty(errorOutput)) {
	        allOutput = allOutput + "STDERR: " + errorOutput;
	      }
	      
	      throw new EDAGIOException(EDAGIOException.NON_ZERO_EXIT_CODE, allOutput, exitVal);
	    } else {
	      logger.debug(output);
	    }
    } catch (IOException e) {
    	logger.warn("Unable to get process output / error: " + e.getMessage());
    }
  }
}
