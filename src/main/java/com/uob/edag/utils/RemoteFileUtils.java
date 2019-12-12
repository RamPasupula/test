package com.uob.edag.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.base.Charsets;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.uob.edag.connection.SshConnectionFactory;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGSSHException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessModel;


/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The file is used executing operations on a file in a remote server.
 */
public class RemoteFileUtils extends AbstractFileUtility {
	
  private String node = null;
  private String key = null;
  private String userName = null;
  private String hostName = null;
  
  private Session openSession() throws EDAGSSHException, EDAGValidationException {
  	Session session = SshConnectionFactory.getFactory().getJschSession(node);

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");
    config.put("kex", "diffie-hellman-group1-sha1," +
                      "diffie-hellman-group14-sha1," + 
    		              "diffie-hellman-group-exchange-sha1," +
                      "diffie-hellman-group-exchange-sha256");
    session.setConfig(config);
    try {
			session.connect();
		} catch (JSchException e) {
			throw new EDAGSSHException(EDAGSSHException.CANNOT_CONNECT_SESSION, session.getHost(), session.getPort(), session.getUserName(), e.getMessage());
		}

    logger.info("Connection to Hadoop Server " + node + (session.isConnected() ? " is established" : " is not established"));
    return session;
  }
  
  private <C extends Channel> C openChannel(Session session, Class<C> type, boolean connect) throws EDAGSSHException {
  	C c = null;
		try {
			Channel chan = null;
			if (ChannelSftp.class.equals(type)) {
				chan = session.openChannel("sftp");
				logger.debug("SFTP channel opened");
			} else if (ChannelExec.class.equals(type)) {
				chan = session.openChannel("exec");
				logger.debug("Exec channel opened");
			} else {
				throw new EDAGSSHException(EDAGSSHException.INVALID_CHANNEL_TYPE, type.getName());
			}
			
			if (type.isAssignableFrom(chan.getClass())) {
				c = type.cast(chan);
				logger.debug(type.getName() + " is assigned from " + chan.getClass().getName());
			} else {
				throw new EDAGSSHException(EDAGSSHException.CHANNEL_NOT_ASSIGNABLE, type.getName(), chan.getClass().getName());
			}
			
			if (connect) {
				c.connect();
				logger.debug("Channel connected");
			}
			
			return c;
		} catch (JSchException e) {
			throw new EDAGSSHException(EDAGSSHException.CANNOT_CONNECT_CHANNEL, type.getName(), e.getMessage());
		}
  }

  public RemoteFileUtils(String node) {
    this.node = StringUtils.trimToEmpty(node);
    
    if (UobConstants.NODE1.equalsIgnoreCase(node.trim())) {
      hostName = PropertyLoader.getProperty(UobConstants.NODE1);
      userName = PropertyLoader.getProperty(UobConstants.NODE1_USER);
      key = PropertyLoader.getProperty(UobConstants.NODE1_PWD);
    } else if (UobConstants.NODE2.equalsIgnoreCase(node.trim())) {
      hostName = PropertyLoader.getProperty(UobConstants.NODE2);
      userName = PropertyLoader.getProperty(UobConstants.NODE2_USER);
      key = PropertyLoader.getProperty(UobConstants.NODE2_PWD);
    } else if (UobConstants.NODE3.equalsIgnoreCase(node.trim())) {
      hostName = PropertyLoader.getProperty(UobConstants.NODE3);
      userName = PropertyLoader.getProperty(UobConstants.NODE3_USER);
      key = PropertyLoader.getProperty(UobConstants.NODE3_PWD);
    }
    
    logger.debug("Remote file utils created: " + this.toString());
  }
  
  public String toString() {
  	return new ToStringBuilder(this).append("node", this.node)
  			                            .append("hostName", this.hostName)
  			                            .append("userName", this.userName)
  			                            .toString();
  }
  
  /**
   * This method is used to read a particular number of lines in a file located on a 
   * remote server.
   * @param fileName The name of the file to be read
   * @param numLines The number of lines to be read from the file
   * @return a List of String with the contents in the file
   * @throws Exception when there is an error reading the lines of the file.
   */
  public List<String> readNLines(String fileName, int numLines, String country) throws EDAGValidationException, EDAGIOException, EDAGSSHException {
  	Session session = null;
    ChannelSftp sftp = null;
    try {
    	session = openSession();
    	sftp = openChannel(session, ChannelSftp.class, true);
    	
      @SuppressWarnings("rawtypes")
			Vector lsRes = null;
			try {
				lsRes = sftp.ls(fileName);
			} catch (SftpException e) {
				throw new EDAGSSHException(EDAGSSHException.CANNOT_LIST_FILES, fileName, session.getHost(), e.getMessage());
			}
			
      if (lsRes == null || lsRes.isEmpty()) {
        throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, fileName, session.getHost());
      }
      logger.debug("File: " + fileName + " exists in host " + session.getHost());
      
      try {
				return readNLines(sftp.get(fileName), numLines, country);
			} catch (SftpException e) {
				throw new EDAGSSHException(EDAGSSHException.CANNOT_GET_FILE, fileName, session.getHost(), e.getMessage());
			}
    } finally {
			if (sftp != null) {
        sftp.disconnect();
			} 
			
			if (session != null) {
        session.disconnect();
			} 
    }
  }
  
  /** 
   * This method is used to read a line from a file located on a remote server using a indicator.
   * @param fileName The Path of the File to be read
   * @param indicator The Indicator of the Line to be read - First/Last
   * @return a String with the content of the line to be read
   * @throws EDAGValidationException 
   * @throws EDAGSSHException 
   * @throws EDAGIOException 
   * @throws Exception when there is an error reading the file
   */
  public String readLineByIndicator(String fileName, String indicator, String charsetName) throws EDAGIOException {
    Session session = null;
    ChannelSftp sftp = null;
    try {
    	session = openSession();
    	sftp = openChannel(session, ChannelSftp.class, true);
    	
      try {
        @SuppressWarnings("rawtypes")
        Vector lsRes = sftp.ls(fileName);
        if (lsRes == null || lsRes.isEmpty()) {
          throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, fileName, session.getHost());
        }
        
        logger.debug("File: " + fileName + " exists in " + session.getHost());
        return super.readLineByIndicator(writeToTempFile(sftp.get(fileName)).getPath(), indicator, charsetName);
      } catch (SftpException excp) {
        throw new EDAGSSHException(EDAGSSHException.CANNOT_GET_FILE, fileName, session.getHost(), excp.getMessage());
      } 
    } catch (EDAGSSHException | EDAGValidationException e) {  
    	throw new EDAGIOException(e.getMessage(), e);
    } finally {
    	if (sftp != null) {
    		sftp.disconnect();
    	}
      
    	if (session != null) {
    		session.disconnect();
    	}
    }
  }
  
  /**
   * This method is used to delete a file located on a remote server .
   * @param sourceFileLoc The location of the Source File
   * @throws EDAGValidationException 
   * @throws Exception when there is an error deleting the file
   */
  public void deleteFile(String sourceFileLoc) throws EDAGSSHException, EDAGValidationException {
    Session session = null;
    ChannelSftp sftp = null;
    try {
    	session = openSession();
    	sftp = openChannel(session, ChannelSftp.class, true);
    	
      try {
        @SuppressWarnings("rawtypes")
        Vector lsRes = sftp.ls(sourceFileLoc);
        if (lsRes == null || lsRes.isEmpty()) {
          throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, sourceFileLoc, session.getHost());
        }
        
        logger.debug("File: " + sourceFileLoc + " exists in " + node + ". Going to delete the file");
        sftp.rm(sourceFileLoc);
      } catch (SftpException excp) {
        if (excp.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
        	throw new EDAGSSHException(EDAGSSHException.CANNOT_LIST_FILES, sourceFileLoc, session.getHost(), excp.getMessage());
        } else {
        	throw new EDAGSSHException(EDAGSSHException.CANNOT_DELETE_FILE, sourceFileLoc, session.getHost(), excp.getMessage());
        }
      }
      
      logger.info("File " + sourceFileLoc + " deleted successfully from: " + session.getHost());
    } finally {
    	if (sftp != null) {
    		sftp.disconnect();
    	}
    	
    	if (session != null) {
    		session.disconnect();
    	}
    }
  }

  /**
   * This method is used to archive a file located on a remote server to a different directory.
   * @param sourceFileLoc The location of the Source File
   * @param archiveFileLoc The Target Location after archival
   * @throws Exception when there is an error archiving the file
   */
  public void archiveFile(String sourceFileLoc, String archiveFileLoc, boolean deleteSourceFile) throws EDAGValidationException, EDAGSSHException {
  	if (!deleteSourceFile) {
  		logger.warn(getClass().getName() + " class ignores deleteSourceFile flag");
  	}
  	
    Session session = null;
    ChannelSftp sftp = null;
    ChannelExec exec = null;
    try {
			session = openSession();
    	sftp = openChannel(session, ChannelSftp.class, true);
    	exec = openChannel(session, ChannelExec.class, false);
    	
      @SuppressWarnings("rawtypes")
			Vector lsRes;
			try {
				lsRes = sftp.ls(sourceFileLoc);
			} catch (SftpException e1) {
				throw new EDAGSSHException(EDAGSSHException.CANNOT_LIST_FILES, sourceFileLoc, session.getHost(), e1.getMessage());
			}
      if (lsRes == null || lsRes.isEmpty()) {
        throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, sourceFileLoc, session.getHost());
      } 
      
      String previousDir = null;
    	previousDir = archiveFileLoc.substring(0, archiveFileLoc.lastIndexOf("/"));
    	
      @SuppressWarnings("rawtypes")
			Vector lsRes2;
			try {
				lsRes2 = sftp.ls(previousDir);
			} catch (SftpException e1) {
				throw new EDAGSSHException(EDAGSSHException.CANNOT_LIST_FILES, previousDir, session.getHost(), e1.getMessage());
			}
      if (lsRes2 == null || lsRes2.isEmpty()) {
        throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, previousDir, session.getHost());
      }
      
      try {
        String zippedArchiveFileLoc = archiveFileLoc + ".gz";
        
        @SuppressWarnings("rawtypes")
        Vector archlsRes = sftp.ls(zippedArchiveFileLoc);
        if (archlsRes != null && !archlsRes.isEmpty()) {
          logger.debug(zippedArchiveFileLoc + " already found on " + session.getHost() + ". Renaming the file with current timestamp");
          long now = System.currentTimeMillis();
          SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
          String newArchiveFileLoc = zippedArchiveFileLoc + "." + sdf.format(now);
          sftp.rename(zippedArchiveFileLoc, newArchiveFileLoc);
        }
      } catch (SftpException excp) {
        logger.warn(archiveFileLoc + " doesn't exist in " + node);
      }

      try {
      	sftp.rename(sourceFileLoc, archiveFileLoc);
      	logger.debug("Archived file successfully from: " + sourceFileLoc + " to: " + archiveFileLoc);
      } catch (SftpException e) {
      	throw new EDAGSSHException(EDAGSSHException.CANNOT_RENAME_FILE, sourceFileLoc, archiveFileLoc, session.getHost(), e.getMessage());
      }
      
      logger.debug("Going to Gzip the file: " + archiveFileLoc);
      exec.setCommand("gzip " + archiveFileLoc);
      try {
				exec.connect();
			} catch (JSchException e) {
				throw new EDAGSSHException(EDAGSSHException.CANNOT_CONNECT_CHANNEL, ChannelExec.class.getName(), e.getMessage());
			}

      logger.debug("Starting output stream of gzip command");
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getInputStream(), Charsets.UTF_8))) { 
	      String line = null;
	      int index = 0;
	      while ((line = reader.readLine()) != null) {
	        logger.debug(++index + ": " + line);
	      }
      } catch (IOException e) {
      	// just log the message
				logger.warn("Unable to read output stream of gzip command: " + e.getMessage());
			} 
      logger.debug("End of output stream of gzip command");

      logger.debug("Staring error stream of gzip command");
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(exec.getErrStream(), Charsets.UTF_8))) { 
      	String line = null;
	      int index = 0;
	      while ((line = reader.readLine()) != null) {
	        logger.info(++index + ": " + line);
	      }
      } catch (IOException e) {
      	// just log the message
      	logger.warn("Unable to read error stream of gzip command: " + e.getMessage());
      }
      logger.debug("End of error stream of gzip command");
      
      int exitStatus = exec.getExitStatus();
      if (exitStatus != 0) {
      	throw new EDAGSSHException(EDAGSSHException.NON_ZERO_EXIT_CODE, "Gzip command failed on the archive file: " + archiveFileLoc, exitStatus);
      }
      
      logger.info("File " + archiveFileLoc + " archived and zipped successfully");
    } finally {
    	if (exec != null) {
    		exec.disconnect();
    	}
      
    	if (sftp != null) {
    		sftp.disconnect();
    	}
      
    	if (session != null) {
    		session.disconnect();
    	}
    }
  }
  
  /**
   * This method is used to move a file located on a remote server to a different directory.
   * @param sourceFileLoc The location of the Source File
   * @param newFileLoc The Target Location after archival
   * @throws Exception when there is an error archiving the file
   */
  public void moveFile(String sourceFileLoc, String newFileLoc) throws EDAGValidationException, EDAGSSHException {
    Session session = null;
    ChannelSftp sftp = null;
    try {
      session = openSession();
      sftp = openChannel(session, ChannelSftp.class, true);

      try {
        @SuppressWarnings("rawtypes")
        Vector lsRes = sftp.ls(sourceFileLoc);
        if (lsRes == null || lsRes.isEmpty()) {
          throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, sourceFileLoc, session.getHost());
        }
      } catch (SftpException excp) {
        throw new EDAGSSHException(EDAGSSHException.CANNOT_LIST_FILES, sourceFileLoc, session.getHost(), excp.getMessage());
      }
      
      String processingDir = null;
      try {
        processingDir = newFileLoc.substring(0, newFileLoc.lastIndexOf("/"));
        
        @SuppressWarnings("rawtypes")
        Vector lsRes2 = sftp.ls(processingDir);
        if (lsRes2 == null || lsRes2.isEmpty()) {
          throw new EDAGSSHException(EDAGSSHException.DIR_NOT_FOUND, processingDir, session.getHost());
        }
      } catch (SftpException excp) {
      	throw new EDAGSSHException(EDAGSSHException.CANNOT_LIST_FILES, processingDir, session.getHost(), excp.getMessage());
      }

      try {
				sftp.rename(sourceFileLoc, newFileLoc);
				logger.debug("Moved file successfully from: " + sourceFileLoc + " to: " + newFileLoc);
			} catch (SftpException e) {
				throw new EDAGSSHException(EDAGSSHException.CANNOT_RENAME_FILE, sourceFileLoc, newFileLoc, session.getHost(), e.getMessage());
			}
    } finally {
      if (sftp != null) {
        sftp.disconnect();
      }
      
      if (session != null) {
        session.disconnect();
      }
    }
  }

  public void copyFile(String fileName, String bizDate) throws EDAGIOException {
  	String command = PropertyLoader.getProperty(UobConstants.CMD_COPY_FILE);
  	command = "./" + command.replace(UobConstants.FILENM_PARAM, fileName)
														.replace(UobConstants.BIZ_DATE_PARAM, bizDate)
														.replace(UobConstants.NODE_HOST_PARAM, hostName)
														.replace(UobConstants.NODE_KEY_PARAM, key)
														.replace(UobConstants.NODE_USER_PARAM, userName);
  	
		executeCommand(command);
  }
  
  public String[] removeHeaderAndFooter(String fileName, String bizDate, String charsetName) throws EDAGIOException {
  	logger.warn(getClass().getName() + ".removeHeaderAndFooter(String, String) returns null Strings. Retrieve header and footer by calling readLineByIndicator(String, String, String, String) prior to calling this method");
  	String command = PropertyLoader.getProperty(UobConstants.CMD_REMOVE_HDER_FTER);
  	command = "./" + command.replace(UobConstants.FILENM_PARAM, fileName)
														.replace(UobConstants.BIZ_DATE_PARAM, bizDate)
														.replace(UobConstants.NODE_HOST_PARAM, hostName)
														.replace(UobConstants.NODE_KEY_PARAM, key)
														.replace(UobConstants.NODE_USER_PARAM, userName);
  	
		executeCommand(command);
		
		return new String[] {null, null};
  }
  
  /**
   * This method is used to remove control characters from last line of a file.
   * @param fileName The Path of the file to be read
   * @throws EDAGSSHException 
   * @throws EDAGValidationException 
   * @throws Exception when there is an error in the script
   */
  protected boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel,
		   HadoopModel destInfo, String charsetName, boolean isSkipErrRecordsEnabled, Map<String, String> charReplacementMap, int linesToValidate) throws EDAGIOException, EDAGValidationException, EDAGSSHException {
  	if (MapUtils.isNotEmpty(charReplacementMap)) {
  		logger.warn("Character replacement map is ignored by " + getClass().getName());
  	}
  	
  	int linesToScan = linesToValidate;
  	if (linesToScan < 0) {
  		logger.warn("Lines to validate must be a positive integer. Defaulting to 11");
  		linesToScan = 11;
  	}
  	
  	StringBuilder fixedWidths = new StringBuilder();
    String layout = sourceInfo.getSourceFileLayoutCd();
    if (UobConstants.FIXED_FILE.equalsIgnoreCase(layout) || UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(layout)) {
      Map<Integer, FieldModel> fieldsInfo = destInfo.getDestFieldInfo();
      int size = fieldsInfo.size();
      for (int j = 1; j <= size; j++) {
        FieldModel field = fieldsInfo.get(j);
        int colLen = field.getLength();
        if (fixedWidths.length() > 0) {
          fixedWidths.append(UobConstants.SPACE);
        }
        
        fixedWidths.append(colLen);
      }
    }
  	
    String command = PropertyLoader.getProperty(UobConstants.CMD_REMOVE_CTRL_CHAR);
    command = "./" + command.replace(UobConstants.FILENM_PARAM, fileName)
            								.replace(UobConstants.NODE_HOST_PARAM, hostName)
            								.replace(UobConstants.NODE_KEY_PARAM, key)
            								.replace(UobConstants.NODE_USER_PARAM, userName)
            								.replace(UobConstants.DELIMIT_CONV_PARAM, String.valueOf(UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(layout)))
            								.replace(UobConstants.CHARSET_PARAM, charsetName)
            								.replace(UobConstants.FIXED_WIDTHS_PARAM, fixedWidths);

		executeCommand(command);
		
    // Validate the File
    logger.debug("Going to read first " + linesToValidate + " lines of the file");
    List<String> nlines = readNLines(sourceInfo.getSourceDirectory(), linesToScan, charsetName);
    
    int actualLineLength = 0;
    int expectedNumFields = 0;
    String sourceFileLayoutCd = StringUtils.trimToEmpty(sourceInfo.getSourceFileLayoutCd());
    if (UobConstants.FIXED_FILE.equalsIgnoreCase(sourceFileLayoutCd)) {
      List<FieldModel> srcFieldModelList = sourceInfo.getSrcFieldInfo();
      if (srcFieldModelList != null) {
        for (FieldModel field : srcFieldModelList) {
          int endPos = field.getEndPosition();
          if (actualLineLength < endPos) {
            actualLineLength = endPos;
          }
        }
      }
    } else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(sourceFileLayoutCd) || 
    		       UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(sourceFileLayoutCd)) {
      Map<Integer, FieldModel> srcFieldModelList = destInfo.getDestFieldInfo();
      if (srcFieldModelList != null) {
        expectedNumFields = srcFieldModelList.size();
      }
    }

    int lineNum = 0;
    int numLinesSize = nlines.size();
    for (String line : nlines) {
      if (UobConstants.CTRL_INFO_HT.equalsIgnoreCase(sourceInfo.getControlInfo()) && 
      		(lineNum == 0 || (numLinesSize < linesToScan && lineNum == numLinesSize - 1))) {
        lineNum++;
        continue; // Header / footer Line
      }
      
      if (UobConstants.FIXED_FILE.equalsIgnoreCase(sourceFileLayoutCd)) {
      	int lineLength = line.length();
        logger.debug("Line in which length is checked: '" + line + "', line length is " + lineLength);
        if (lineLength != actualLineLength) {
          throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH, lineLength, actualLineLength, "Line is " + line);
        } else {
          logger.debug("Line Length is: " + lineLength + " & expected is: " + actualLineLength + ", Match");
        }
      } else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(sourceFileLayoutCd) || 
      		       UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(sourceFileLayoutCd)) {
        String colDelimiter = StringUtils.trimToEmpty(sourceInfo.getColumnDelimiter());
        colDelimiter = colDelimiter.replaceAll("\\|", "\\\\\\|");
        
        String txtDelimiter = sourceInfo.getTextDelimiter();
        String delim = StringUtils.isNotEmpty(txtDelimiter) ? txtDelimiter + colDelimiter + txtDelimiter : colDelimiter; 

        String[] splitLine = line.split(delim, -1);
        int numFields = splitLine.length;
        if (numFields != expectedNumFields) {
          throw new EDAGValidationException(EDAGValidationException.FIELD_COUNT_MISMATCH, numFields, expectedNumFields, 
          		                              "Field delimiter is " + delim);
        } else {
          logger.debug("Number of fields is " + numFields + " and expected is " + expectedNumFields + ", Match");
        }
      }
      
      lineNum++;
    }
    return false;
  }

  /**
   * This method is used to read a file located on a remote server.
   * @param fileName The Path of the file to be read
   * @return a list of String with the contents of the file
   * @throws EDAGValidationException 
   * @throws EDAGSSHException 
   * @throws EDAGIOException 
   * @throws Exception when there is an error reading the file
   */
  public List<String> readFile(String fileName, String charsetName) throws EDAGSSHException, EDAGValidationException, EDAGIOException {
  	List<String> result = null;
    Session session = null;
    ChannelSftp sftp = null;
    try {
      session = openSession();
      sftp = openChannel(session, ChannelSftp.class, true);
      
      try {
        @SuppressWarnings("rawtypes")
        Vector lsRes = sftp.ls(fileName);
        if (lsRes == null || lsRes.isEmpty()) {
          throw new EDAGSSHException(EDAGSSHException.FILE_NOT_FOUND, fileName, session.getHost());
        }
        
        logger.debug("File: " + fileName + " exists on Hadoop Server. Going to read the file");
        result = super.readFile(sftp.get(fileName), charsetName);
      } catch (SftpException excp) {
        throw new EDAGSSHException(EDAGSSHException.CANNOT_GET_FILE, fileName, session.getHost(), excp.getMessage());
      } 
    } finally {
      if (sftp != null) {
        sftp.disconnect();
      }
      
      if (session != null) {
        session.disconnect();
      }
    }
    
    return result;
  }
  
  /**
   * This method is used to execute a shell script located on a remote server.
   * @param cmd The fully qualified path of shell script with input arguments 
   *     if any as a complete command string
   * @return a result of the script execution which are printed out in the console
   * @throws Exception when there is an error in executing the script
   */
  public int sshConnectAndExecuteCommand(String cmd) throws EDAGSSHException, EDAGValidationException {
    Session session = null;
    ChannelExec channelExec = null;
    try {
      session = openSession();
      channelExec = openChannel(session, ChannelExec.class, false);
      
      //Set the command to be executed
      channelExec.setCommand(cmd);
      
      logger.debug("Going to execute the command: '" + cmd + "'");
      // Execute the command
      channelExec.connect();
      
      try {
	      // Read the output from the input Stream
	      String line = null;
	      StringBuilder buff = new StringBuilder();
	      try (BufferedReader br = new BufferedReader(new InputStreamReader(channelExec.getInputStream()))) {
		      // Read each line from the buffered reader and add it to result list
		      while ((line = br.readLine()) != null) {
		        buff.append(line + UobConstants.LINE_SEPARATOR);
		      }
	      } 
	      
	      if (buff.length() > 0) {
	      	logger.debug("Command output: " + buff.toString());
	      }
	      
	      // Read the output from the error Stream
	      buff = new StringBuilder();
	      try (BufferedReader br = new BufferedReader(new InputStreamReader(channelExec.getErrStream()))) {
		      // Read each line from the buffered reader and add it to result list
		      while ((line = br.readLine()) != null) {
		        buff.append(line + UobConstants.LINE_SEPARATOR);
		      }
	      } 
	      
	      if (buff.length() > 0) {
	      	throw new EDAGSSHException(EDAGSSHException.CANNOT_EXEC_CMD, cmd, buff.toString());
	      }
      } catch (IOException e) {
      	// not throwing exception since the execution exit status is the one that's important
      	logger.warn("Unable to get output / error from command execution: " + e.getMessage());
      }
      
      //retrieve the exit status of the remote command corresponding to this channel
      int exitStatus = channelExec.getExitStatus();
      logger.debug("Command exit status: " + exitStatus);
      return exitStatus;
    } catch (JSchException e) {
    	throw new EDAGSSHException(EDAGSSHException.CANNOT_EXEC_CMD, cmd, e.getMessage());
    } finally {
      if (channelExec != null) {
        channelExec.disconnect();
      }
      
      if (session != null) {
        session.disconnect();
      }
    }
  }

  /**
   * This method is used to write a file  on a remote server.
   * @param fileName The Path of the file to be read locally
   * @param remotePath The Path where the file needs to be copied on remote server
   * @throws Exception when there is an error reading the file
   */
  public void sshConnectAndWriteFile(String remotePath, String fileName) throws EDAGSSHException, EDAGValidationException {
    Session session = null;
    ChannelSftp sftp = null;
    try {
      session = openSession();
      sftp = openChannel(session, ChannelSftp.class, true);
      
      // Changing directory path on remote server  
      sftp.cd(remotePath);
      sftp.put(fileName, remotePath);
      logger.info("Successfully put file " + fileName + " into " + remotePath);
    } catch (SftpException e) {
    	throw new EDAGSSHException(EDAGSSHException.CANNOT_PUT_FILE, fileName, remotePath, session.getHost(), e.getMessage());
    } finally {
      if (sftp != null) {
        sftp.disconnect();
      }
      
      if (session != null) {
        session.disconnect();
      }
    }
  }
  
  
	public void archiveFileOSM(ProcessModel procModel, String targetFilename, boolean deleteSourceFile) throws EDAGIOException{
		
	}
  
}
