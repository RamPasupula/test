package com.uob.edag.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
//import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.io.EDAGFileReader;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessModel;

import jline.internal.Log;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/31/2016
 * @Description : The class contains the common File IO operations.
 * 
 */

public class FileUtils extends AbstractFileUtility {
	
	public static final String BUFFER_SIZE_PROPERTY = FileUtils.class.getName() + ".BufferSize";
	
	private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static final Logger staticLogger = Logger.getLogger(FileUtils.class);
  
  public List<String> readNLines(String fileName, int numLines, String charsetName) throws EDAGIOException {
  	File sourceFile = new File(fileName);
  	if (!sourceFile.isFile()) {
  		throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, sourceFile.getPath(), "Please ensure the file exists");
  	}
  	
    List<String> result = null;
  	try (InputStream is = new FileInputStream(sourceFile)) { 
  		result = this.readNLines(is, 11, charsetName);
  	} catch (FileNotFoundException e) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, sourceFile.getPath(), "Please ensure the file exists");
		} catch (IOException e) {
			logger.warn("Unable to close input stream of " + sourceFile.getPath() + ": " + e.getMessage());
		}

		return result;
	}

	/**
	 * This method is used to write some content into a file.
	 * 
	 * @param input
	 *            A list of strings to be written into the file
	 * @param file
	 *            The File into which the content is to be written
	 * @throws IOException
	 *             when there is an error writing the file
	 */
	public void writeToFile(List<String> input, File file) throws EDAGIOException {
		try (OutputStream out = new FileOutputStream(file)) {
			for (String inputStr : input) {
				out.write(inputStr.getBytes(Charsets.UTF_8));
				out.write(UobConstants.NEWLINE.getBytes(Charsets.UTF_8));
				out.write(UobConstants.NEWLINE.getBytes(Charsets.UTF_8));
			}

			logger.debug(input.size() + " lines written into " + file.getPath());
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_WRITE_FILE, file.getPath(), e.getMessage());
		}
	}

	/**
	 * This method is used to write some content into a file.
	 * 
	 * @param input
	 *            A string to be written into the file
	 * @param file
	 *            The File into which the content is to be written
	 * @throws IOException
	 *             when there is an error writing the file
	 */
	public void writeToFile(String input, File file) throws EDAGIOException {
		List<String> inputList = new ArrayList<String>();
		inputList.add(input);
		writeToFile(inputList, file);
	}

	public static boolean isDirWriteable(File dir) {
		boolean result = false;

		try {
			File tempFile = null;
			try {
				tempFile = File.createTempFile("isDirWriteable", ".test", dir);
				tempFile.deleteOnExit();

				org.apache.commons.io.FileUtils.write(tempFile, "The quick brown fox jumps over the lazy dog",
						Charset.defaultCharset());
				result = true;
				staticLogger.debug(dir.getPath() + " is a writeable directory. No SSH required");
			} finally {
				if (tempFile != null) {
					tempFile.delete();
				}
			}
		} catch (IOException e) {
			staticLogger.warn("Unable to create temporary file in " + dir.getPath() + ", assuming SSH is required");
		}
  	
  	return result;
  }
  
  public static void gzipFile(File source, File target) throws EDAGIOException {
  	try (FileInputStream is = new FileInputStream(source)) {
	  	try (GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(target))) {
				
	  		try {
	  			byte[] buff = new byte[2048];
	  			int bytesRead = -1;
	  			
	  			while ((bytesRead = is.read(buff)) > 0) {
	  				gos.write(buff, 0, bytesRead);
	  			}
	  			
	  			staticLogger.debug(source.getPath() + " gzipped to " + target.getPath());
	  		} catch (IOException e) {
	  			String err = "Unable to gzip " + source.getPath() + " to " + target.getPath() + ": " + e.getMessage();
	  			staticLogger.error(err);
	  			throw new EDAGIOException(err, e);
	  		}
	  	} catch (IOException e1) {
				throw new EDAGIOException(EDAGIOException.CANNOT_GZIP_FILE, source.getPath(), target.getPath(), e1.getMessage());
			}
  	} catch (FileNotFoundException e2) {
			throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, source.getPath(), e2.getMessage());
		} catch (IOException e2) {
			staticLogger.warn("Unable to close input stream from file " + source.getPath() + ": " + e2.getMessage());
		} 
  }
  
  public void copyFile(String sourceFilename, String targetFilename) throws EDAGIOException {
  	File source = new File(sourceFilename);
  	File target = new File(targetFilename);
  	
  	try {
			org.apache.commons.io.FileUtils.copyFile(source, target);
			logger.info(source.getPath() + " copied to " + target.getPath());
  	} catch (IOException e) {
  		throw new EDAGIOException(EDAGIOException.CANNOT_COPY_FILE, source.getPath(), target.getPath(), e.getMessage());
		}
  }
  
  public void moveFile(String source, String target) throws EDAGIOException {
  	File sourceFile = new File(source);
  	File targetFile = new File(target);
  	
  	try {
			org.apache.commons.io.FileUtils.moveFile(sourceFile, targetFile);
			logger.debug(sourceFile.getPath() + " moved to " + targetFile.getPath());
  	} catch (IOException e) {
  		throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, sourceFile.getPath(), targetFile.getPath(), e.getMessage());
		}
	}

	public void archiveFile(String sourceFilename, String targetFilename, boolean deleteSourceFile) throws EDAGIOException {
		File source = new File(sourceFilename);
		File target = new File(targetFilename);

		if (!source.isFile()) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, source.getPath(), "File doesn't exist");
		}

		File targetDir = target.getParentFile();
		if (!targetDir.isDirectory()) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, targetDir.getPath(), "Directory doesnt' exist");
		}

		File targetFile = new File(target.getParentFile(), target.getName() + ".gz");
		if (targetFile.isFile()) {
			File renameTarget = new File(targetFile.getParentFile(),
					targetFile.getName() + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
			try {
				Files.move(targetFile.toPath(), renameTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
				logger.info(targetFile.getPath() + " renamed to " + renameTarget.getPath());
			} catch (IOException e) {
				throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, targetFile.getPath(), renameTarget.getPath(),
						e.getMessage());
			}
  	}
  	
  	boolean closingReader = false;
  	boolean closingWriter = false;
		try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(targetFile))) {
			try (FileInputStream in = new FileInputStream(source)) { 
				byte[] buff = new byte[2048];
		  	int bytesRead = -1;
		  	
				while ((bytesRead = in.read(buff, 0, 2048)) > 0) {
					out.write(buff, 0, bytesRead);
				}
				
				closingReader = true;
			} 
			
			closingWriter = true;
		} catch (IOException e) {
			if (!closingReader && !closingWriter) {
				throw new EDAGIOException(EDAGIOException.CANNOT_GZIP_FILE, source.getPath(), targetFile.getPath(), e.getMessage());
			} else {
				logger.warn("Unable to close input stream of " + source.getPath() + " or output stream of " + targetFile.getPath()
						+ ": " + e.getMessage());
			}
  	}
		
		logger.info(targetFile.getPath() + " created");
		
		if (deleteSourceFile) {
			if (source.delete()) {
				logger.info(source.getPath() + " deleted");
			} else {
				logger.warn(source.getPath() + " cannot be deleted, its file handle might be held by another process");
			}
		}
  }

	/**
	 * This method is used to delete a file in a given location.
	 * 
	 * @param fileLocation
	 *            The Location of the file to be deleted
	 * @throws Exception
	 *             when there is an error deleting the file
	 */
	public void deleteFile(String fileLocation) throws EDAGIOException {
		File file = new File(fileLocation);

		if (!file.isFile()) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, file.getPath(), "File is not a local file");
		}

		boolean deleted = file.delete();
		if (!deleted) {
			throw new EDAGIOException(EDAGIOException.CANNOT_DELETE_FILE, fileLocation,
					"The file might be non-existent or locked by a process");
		} else {
			logger.debug(fileLocation + " deleted");
		}
	}

	/**
	 * This method is used to encode the contents of a file to Base 64.
	 * 
	 * @param file
	 *            The file whose contents is to be encoded
	 * @return Base 64 encoded string
	 * @throws IOException
	 *             when there is an error during the encoding process
	 */
	public String encodeFileToBase64Binary(File file) throws EDAGIOException {
		byte[] bytes = convertFileToBytes(file);
    byte[] encoded = Base64.encodeBase64(bytes);
    String encodedString = new String(encoded, Charsets.UTF_8);

    return encodedString;
  }

	/**
	 * This method is used to read the contents of a file into byte array.
	 * 
	 * @param file
	 *            The file object whose contents is to be read
	 * @return Byte array with the contents of the file
	 * @throws IOException
	 *             when there is an error reading the file.
	 */
	private byte[] convertFileToBytes(File file) throws EDAGIOException {
		try (InputStream is = new FileInputStream(file)) {
			long length = file.length();
			if (length > Integer.MAX_VALUE) {
				throw new EDAGIOException(EDAGIOException.FILE_TOO_LARGE, file.getPath(), length, Integer.MAX_VALUE);
			}

			byte[] bytes = new byte[new Long(length).intValue()];

      int offset = 0;
      int numRead = 0;
      while (offset < bytes.length && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
        offset += numRead;
      }

      if (offset < bytes.length) {
        throw new EDAGIOException("Could not completely read file " + file.getName());
      }
      
      return bytes;
    } catch (FileNotFoundException e) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, file.getPath(), e.getMessage());
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, file.getPath(), e.getMessage());
		} 
  }
  
  public List<String> readFile(String fileName, String charsetName) throws EDAGIOException {
  	File file = new File(fileName);
    if (!file.isFile()) {
    	throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, fileName, "File is not a local file");
    }
    
    List<String> result = null;
    try (InputStream is = new FileInputStream(file)) {
    	result = this.readFile(is, charsetName);
    } catch (FileNotFoundException e) {
    	throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, fileName, "File is not a local file");
		} catch (IOException e) {
			Log.warn("Unable to close input stream of file " + fileName + ": " + e.getMessage());
		}
    
    return result;
  }

	public String[] removeHeaderAndFooter(String fileName, String bizDate, String charsetName) throws EDAGIOException {
  	String[] headerAndFooter = new String[2];
  	File backupFile = new File(fileName + "." + bizDate);
  	File dataFile = new File(fileName);
  	
  	try {
			Files.move(dataFile.toPath(), backupFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			logger.debug(dataFile.getPath() + " renamed to " + backupFile.getPath());
		} catch (IOException e1) {
			throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, dataFile.getPath(), backupFile.getPath(), e1.getMessage());
		}

		try (LineNumberReader reader = new LineNumberReader(
				new BufferedReader(new InputStreamReader(new FileInputStream(backupFile), charsetName)))) {
			try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataFile), charsetName))) {
				String line = null;
				String previousLine = null;
				String prev2Line = null;

				try {
					while ((line = reader.readLine()) != null) {
						if (previousLine == null) {
							// this is the header
							headerAndFooter[0] = line;
						}

						if (previousLine != null && prev2Line != null && !line.startsWith(UobConstants.SUBSTITUTE_CHAR)) {
							writer.write(previousLine + System.lineSeparator());
						}

						prev2Line = previousLine;
						previousLine = line;
					}

					if (previousLine != null && !previousLine.startsWith(UobConstants.SUBSTITUTE_CHAR)) {
						// this is the footer
						headerAndFooter[1] = previousLine;
					} else {
						headerAndFooter[1] = prev2Line;
					}
				} catch (IOException e) {
					throw new EDAGIOException(EDAGIOException.CANNOT_READ_WRITE_FILE, backupFile.getPath(), dataFile.getPath(),
							e.getMessage());
				}
			}
		} catch (IOException e) {
			logger.warn("Unable to close reader of file " + backupFile.getPath() + " or writer to file " + dataFile.getPath() + ": "
					+ e.getMessage());
		}
  	
  	logger.debug("Header line: " + headerAndFooter[0]);
  	logger.debug("Footer line: " + headerAndFooter[1]);
  	return headerAndFooter;
  }



	/**
	 * Remove Control Character and does new line management
	 * @param fileName incoming file
	 * @param outputFile output file with _1 suffix
	 * @param sourceInfo file model obj
	 * @param controlModel control model obj
	 * @param destInfo hadoop model obj
	 * @param charsetName charset name
	 * @param isSkipErrRecordsEnabled skip error record flag
	 * @param charReplacementMap char replacement map
	 * @param linesToValidate number of lines to validate
	 * @return if the file has bad lines. Only if isSkipErrorRecords is enabled.
	 * @throws EDAGIOException
	 * @throws EDAGValidationException
	 */
  protected boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel,
										   HadoopModel destInfo, String charsetName, boolean isSkipErrRecordsEnabled, Map<String, String> charReplacementMap, int linesToValidate) throws EDAGIOException, EDAGValidationException {
  	File inputFile = new File(fileName);
  	if (!inputFile.isFile()) {
  		throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, inputFile.getPath(), "Input file is not a valid file");
  	}
  	
  	File targetFile = new File(outputFile);
  	File errorFile = new File (outputFile + "_0"); // To collect the error records, suffixed by name _0
  	
  	Charset charset = charsetName == null ? Charset.forName(PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET)) : Charset.forName(charsetName);
  	
  	Map<Pattern, String> controlCharacterPatternMap = null;
		if (MapUtils.isNotEmpty(charReplacementMap)) {
			controlCharacterPatternMap = new HashMap<Pattern, String>();
			for (String controlCharacters : charReplacementMap.keySet()) {
				Pattern pattern = Pattern.compile(controlCharacters);
				String replacement = charReplacementMap.get(controlCharacters);
				controlCharacterPatternMap.put(pattern, replacement);
				logger.info("Pattern " + controlCharacters + " will be replaced by '" + replacement + "'");
			}
		} else {
			logger.info("Skipping control characters replacement since there's no pattern defined ");
		}
  	
		SortedMap<Integer, FieldModel> fieldMap = new TreeMap<Integer, FieldModel>();
		fieldMap.putAll(destInfo.getDestFieldInfo());
		String delim = StringUtils.trimToEmpty(sourceInfo.getColumnDelimiter());
		String quote = StringUtils.trimToEmpty(sourceInfo.getTextDelimiter());
		
		String buffSizeStr = PropertyLoader.getProperty(BUFFER_SIZE_PROPERTY);
		Integer buffSize = StringUtils.isBlank(buffSizeStr) ? null : Integer.parseInt(buffSizeStr);
 		boolean endOfReader = false;
		boolean endOfWriter = false;
		String line = null;
		String footerLineString = "";
	    boolean hasBadLines = false;
		if(controlModel != null)
			footerLineString = controlModel.getFooterLine();
		try (EDAGFileReader reader = createEDAGFileReader(inputFile, charset, footerLineString, controlCharacterPatternMap, buffSize)) {
			try (BufferedWriter writer = createBufferedWriter(new OutputStreamWriter(new FileOutputStream(targetFile), charset), buffSize);
					BufferedWriter errorRecords = createBufferedWriter(new OutputStreamWriter(new FileOutputStream(errorFile), charset), buffSize)) {
				if (UobConstants.FIXED_FILE.equalsIgnoreCase(sourceInfo.getSourceFileLayoutCd())) {
					int lastFieldEndPosition = fieldMap.get(fieldMap.lastKey()).getEndPosition();
					while ((line = reader.readLine()) != null) {
						if (linesToValidate < 0 || reader.getLineNumber() <= linesToValidate || isSkipErrRecordsEnabled) {
							// we check if current line has the same length as last field end position
							// if not, we assume current line is split by \n character
							// keep adding the subsequent line(s) to current line (replace \n with space) until current line's length is the same as last field end position
							String nextLine = null;

							while (line.length() < lastFieldEndPosition && ((nextLine = reader.readLine()) != null)) {
								// JIRA EDF-106
								if (linesToValidate > 0 && !isSkipErrRecordsEnabled) {
									throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH, line.length(), lastFieldEndPosition, "The whole file should be scanned for control characters");
								}
								
								// Batch 8 - skip and collect error record.
								if(!isSkipErrRecordsEnabled) {
									 logger.debug("Line[" + reader.getLineNumber() + "] length (" + line.length() + ") is less than field end position (" + lastFieldEndPosition + "), adding next line (" + nextLine.length() + " chars) to line");
									line += " " + nextLine;
								} else {
									errorRecords.write(line + LINE_SEPARATOR);
									hasBadLines = true;
									if(nextLine.length() < lastFieldEndPosition) {
										errorRecords.write(nextLine + LINE_SEPARATOR);
									} else {
										writer.write(nextLine + LINE_SEPARATOR);
									}
									line = null;
									break;
								}
							}
							
							if (line != null && line.length() != lastFieldEndPosition && !isSkipErrRecordsEnabled) {
								// we reach EOF and line length is still not the same as last field's end position
								throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH, line.length(), lastFieldEndPosition, "Line number: " + reader.getLineNumber() + ", line: " + line);
							} else if (line != null && line.length() >= lastFieldEndPosition) {
								writer.write(line + LINE_SEPARATOR);
							} else if( line != null) {
								hasBadLines = true;
								errorRecords.write(line + LINE_SEPARATOR);
							}
						}
					}
				} else if (UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(sourceInfo.getSourceFileLayoutCd())) {
					/* The delimiter for FB defined in framework-conf.properties is \\u0007. Notice that the bell character (\u0007) is escaped with another backslash. 
					 * This is because when we want to parse a string using Java regular expression, a backslash needs to be escaped with another backslash.
           			* In FB layout case, we want to concatenate fields with bell character, not parse fields. So the value should be \u0007 instead.
           			* However, it would be confusing for the user to see B delimiter defined as \\u0007, and FB delimiter defined as \u0007 in the properties file.
           			* To maintain uniformity, we still put \\u0007 as FB delimiter, but when we use it to concatenate fields, the value is unescaped (the extra backslash is removed).
					 */
					String unescapedDelim = StringEscapeUtils.unescapeJava(delim);
					int lastFieldEndPosition = fieldMap.get(fieldMap.lastKey()).getEndPosition();
					while ((line = reader.readLine()) != null) {
						if (linesToValidate < 0 || reader.getLineNumber() <= linesToValidate || isSkipErrRecordsEnabled) {
							String nextLine = null;
							while (line.length() < lastFieldEndPosition && ((nextLine = reader.readLine()) != null)) {
								if (linesToValidate > 0 && !isSkipErrRecordsEnabled) {
									throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH, line.length(), lastFieldEndPosition, "The whole file should be scanned for control characters");
								}
								
								if(!isSkipErrRecordsEnabled) {
									logger.debug("Line[" + reader.getLineNumber() + "] length (" + line.length() + ") is less than field end position (" + lastFieldEndPosition + "), adding next line (" + nextLine.length() + " chars) to line");
									line += " " + nextLine;
								} else {
									errorRecords.write(line + LINE_SEPARATOR);
									hasBadLines = true;
									if(nextLine.length() < lastFieldEndPosition) {
										errorRecords.write(nextLine + LINE_SEPARATOR);
										line = null;
									} else {
										line = nextLine;
									}
									break;
								}
							}
							String targetLine = null;
							 if (line != null && lastFieldEndPosition != line.length() && !isSkipErrRecordsEnabled) {
								throw new EDAGValidationException(EDAGValidationException.LINE_LENGTH_MISMATCH, line.length(),  lastFieldEndPosition, "Line no: " + reader.getLineNumber() + ", line: " + line);
							} else if(line != null && line.length() == lastFieldEndPosition) {
								targetLine = "";
								for (FieldModel field : fieldMap.values()) {
									String value = quote + StringUtils.trimToEmpty(line.substring(field.getStartPosition(), field.getEndPosition())) + quote;
									targetLine += value + unescapedDelim ;
								}
							} else if(line != null) {
								hasBadLines = true;
								errorRecords.write(line + LINE_SEPARATOR);
							}
							if( targetLine != null) {
								writer.write(targetLine + LINE_SEPARATOR);
							}
						}
					}
					logger.debug("Fixed width file " + inputFile.getPath() + " converted to delimited file " + targetFile.getPath());
				} else if (UobConstants.DELIMITED_FILE.equalsIgnoreCase(sourceInfo.getSourceFileLayoutCd())) { 
					// delimited file
					String escapedDelim = delim.replaceAll("\\|", "\\\\\\|");
					while ((line = reader.readLine()) != null) {
						String nextLine = null;
						String[] fields = line.split(escapedDelim, -1);
						
						if (linesToValidate < 0 || reader.getLineNumber() <= linesToValidate || isSkipErrRecordsEnabled) {
							// we check if current line contains the correct number of fields.
							// If not, we assume current line is split by \n character.
							// keep adding the subsequent line(s) to current line (replace \n with space) and parse the current line 
							// again until the number of fields matches expected number of fields.
							while (fields.length < fieldMap.size() && ((nextLine = reader.readLine()) != null)) {
								// JIRA EDF-106
								if (linesToValidate > 0 && !isSkipErrRecordsEnabled) {
									throw new EDAGValidationException(EDAGValidationException.FIELD_COUNT_MISMATCH, fields.length, fieldMap.size(), "The whole file should be scanned for control characters");
								}
								
								if(!isSkipErrRecordsEnabled){
									logger.debug("Parsed field count (" + fields.length + ") is less than expected field count(" + fieldMap.size() + "), adding next line (" + nextLine.length() + " chars) to line[" + reader.getLineNumber() + "]");
									line = line + " " + nextLine;
									fields = line.split(escapedDelim, -1);
									logger.debug("New field count is " + fields.length);
								} else {
									errorRecords.write(line + LINE_SEPARATOR);
									hasBadLines = true;
									String [] splits = nextLine.split(escapedDelim, -1);
									if(splits.length < fieldMap.size() || splits.length > fieldMap.size() ) {
										errorRecords.write(nextLine + LINE_SEPARATOR);
									} else {
										writer.write(nextLine + LINE_SEPARATOR);
									}
									line = null;
									break;
								}
							}
							
							if (fields.length != fieldMap.size() && !isSkipErrRecordsEnabled) {
								// we reach EOF and number of fields parsed from current line is still less than expected number of fields
								throw new EDAGValidationException(EDAGValidationException.FIELD_COUNT_MISMATCH, fields.length, fieldMap.size(), "Line no: " + reader.getLineNumber() + ", line: " + line);
							} else if(fields.length == fieldMap.size()) {
								writer.write(line + LINE_SEPARATOR);
							} else if( line != null) {
								hasBadLines = true;
								errorRecords.write(line + LINE_SEPARATOR);
							}
						}
					}
				} // this is for REGX scenario, new line management
				//else if (UobConstants.REG_EXPRESSION.equalsIgnoreCase(sourceInfo.getSourceFileLayoutCd())) {
				else if (getIfPersoneticsWebLogsProcessing(sourceInfo)) {	
          
					logger.debug("removeControlCharacter: goting to execute regular expression branch");

					String field_value = "";
					int noOfRecords = 0;
					linesToValidate = -1;
					String reg_prefix_expression = PropertyLoader.getProperty(UobConstants.PERSONETICS_PREFIX_REGX);
					
					logger.debug("reg_prefix_expression: " + reg_prefix_expression);

					List<FieldModel> filedModelList = new ArrayList<FieldModel>();
					filedModelList = sourceInfo.getSrcFieldInfo();
					Collections.sort(filedModelList);
					
					LineNumberReader lineReader = null;
					CSVPrinter printer = null;
					CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator(LINE_SEPARATOR);
					
					try {

						lineReader = new LineNumberReader(new InputStreamReader(new FileInputStream(inputFile), charset));
						
						String tempRegx = null;
						boolean firstRowFlag = true;
						String prevousLine = null;
						List<String> record = new ArrayList<>();
						printer = new CSVPrinter(writer, format);
						
						//logger.debug("filedModelList.size(): " + filedModelList.size());

						while ((line = lineReader.readLine()) != null ) {
							
							if (Pattern.compile(reg_prefix_expression, Pattern.CASE_INSENSITIVE).matcher(line).find() && firstRowFlag) {// 1st row - match
								
								prevousLine = line;
								firstRowFlag = false;
								
							}else if (Pattern.compile(reg_prefix_expression, Pattern.CASE_INSENSITIVE).matcher(line).find() && !firstRowFlag){// non 1st row match
								
								for (FieldModel fieldModel : filedModelList) {

									tempRegx = fieldModel.getFieldRegExpression();
									
									//logger.debug("tempRegx : " + field_value);
									//logger.debug("tempRegx : " + prevousLine);
									
									Matcher macher = Pattern.compile(tempRegx, Pattern.CASE_INSENSITIVE).matcher(prevousLine);
									if (macher.find()) {
										field_value = macher.group();
									}else {
										field_value = "";
									}
									
									record.add(field_value);
									
									//logger.debug("field_value : " + field_value);
								}
								
								//logger.debug("record.size(): " + record.size());
								//logger.debug("transformed record : " + record);
								
								printer.printRecord(record);
								
								//writer.write(outputLine.substring(0, outputLine.length() - 1) + LINE_SEPARATOR);
								noOfRecords++;
								prevousLine = line;
								field_value = null;
								record.clear();
								
							}else {
								if (firstRowFlag) {
									logger.debug("Line number : " + lineReader.getLineNumber() + " does not match the Regular Expression.");
								}else {
									prevousLine = prevousLine + " " + line;
								}
							}
						}
						
						//output the last line.
						if (prevousLine != null) {

							for (FieldModel fieldModel : filedModelList) {

								tempRegx = fieldModel.getFieldRegExpression();
								Matcher macher = Pattern.compile(tempRegx, Pattern.CASE_INSENSITIVE).matcher(prevousLine);
								if (macher.find()) {
									field_value = macher.group();
								}else {
									field_value = "";
								}
								record.add(field_value);
							}
							
							printer.printRecord(record);
							noOfRecords++;
							field_value = null;
							record.clear();
						}
						
					} catch (FileNotFoundException e) {
						throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, inputFile.getPath(), e.getMessage());

					} finally {
						printer.close();
						lineReader.close();
					}
					
					controlModel.setTotalRecords(noOfRecords);
							
				} else if (checkIfPlainCopy(sourceInfo)) {
					while ((line = reader.readLine()) != null) {
						
						String unescapedDelim = StringEscapeUtils.unescapeJava(delim);
						int fieldLenghts = -1;
						if(UobConstants.REG_EXPRESSION.equalsIgnoreCase(sourceInfo.getSourceFileLayoutCd())) {
							CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator(System.getProperty("line.separator"));
							CSVParser parser = CSVParser.parse(line, format);
							List<CSVRecord> csvRecordsList = parser.getRecords();
							fieldLenghts = csvRecordsList.size() >= 1 ? csvRecordsList.get(0).size(): 0;
						} else {
							String[] fields = line.split(unescapedDelim, -1);
							fieldLenghts = fields.length;
						}
						
						if (linesToValidate < 0 || reader.getLineNumber() <= linesToValidate) {
							if (fieldLenghts != fieldMap.size()) {
								// we reach EOF and number of fields parsed from current line is still less than expected number of fields
								throw new EDAGValidationException(EDAGValidationException.FIELD_COUNT_MISMATCH,fieldLenghts , fieldMap.size(), "Line no: " + reader.getLineNumber() + ", line: " + line);
							} 
						}
						writer.write(line + LINE_SEPARATOR);
					}
				}
				
				endOfWriter = true;
			}
			
			endOfReader = true;
			logger.info((linesToValidate > 0 ? linesToValidate : reader.getLineNumber()) + " line(s) validated successfully");
		} catch (FileNotFoundException e) {
			throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, inputFile.getPath(), e.getMessage());
		} catch (IOException e) {
			if (endOfWriter) {
				// exception occurs when trying to close writer
				logger.warn("Unable to close writer to file " + targetFile.getPath() + ": " + e.getMessage());
			} else if (endOfReader) {
				// exception occurs when trying to close reader
				logger.warn("Unable to close reader of file " + inputFile.getPath() + ": " + e.getMessage());
			} else {
				throw new EDAGIOException("Unable to convert " + inputFile.getPath() + 
						                      " from fixed length to delimited and write the result to " + targetFile.getPath() + ": " + e.getMessage());
			}
		}
		
		return hasBadLines;
  }

	private EDAGFileReader createEDAGFileReader(File inputFile, Charset charset, String footerLine,
			Map<Pattern, String> controlCharacterPatternMap, Integer buffSize) throws FileNotFoundException {
		return buffSize == null
				? new EDAGFileReader(new InputStreamReader(new FileInputStream(inputFile), charset), footerLine,
						controlCharacterPatternMap)
				: new EDAGFileReader(new InputStreamReader(new FileInputStream(inputFile), charset), buffSize, footerLine,
						controlCharacterPatternMap);
	}
	
	private BufferedWriter createBufferedWriter(Writer writer, Integer buffSize) {
		return buffSize == null ? new BufferedWriter(writer) : new BufferedWriter(writer, buffSize);
	}

	private boolean checkIfPlainCopy(FileModel fileModel) {
		return (UobConstants.XLS_FILE_WITH_HEADER.equals(fileModel.getSourceFileLayoutCd())
				|| UobConstants.XLS_FILE_WITHOUT_HEADER.equals(fileModel.getSourceFileLayoutCd())
				|| UobConstants.XLSX_FILE_WITH_HEADER.equals(fileModel.getSourceFileLayoutCd())
				|| UobConstants.XLSX_FILE_WITHOUT_HEADER.equals(fileModel.getSourceFileLayoutCd())
				|| (UobConstants.REG_EXPRESSION.equalsIgnoreCase(fileModel.getSourceFileLayoutCd())
						&& fileModel.getIsWebLogsProcessing()));
	}

	private boolean getIfPersoneticsWebLogsProcessing(FileModel fileModel) {
		return UobConstants.REG_EXPRESSION.equalsIgnoreCase(fileModel.getSourceFileLayoutCd()) && !fileModel.getIsWebLogsProcessing();
	}
	
	// Added by Tyler for OSM
		public void archiveFileOSM(ProcessModel procModel, String targetFilename, boolean deleteSourceFile) throws EDAGIOException {
			
			File source = new File(procModel.getSrcInfo().getSourceDirectory());
			File target = new File(targetFilename);

			if (!source.isFile()) {
				throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, source.getPath(), "File doesn't exist");
			}

			File targetDir = target.getParentFile();
			if (!targetDir.isDirectory()) {
				throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, targetDir.getPath(), "Directory doesnt' exist");
			}

			File targetFile = new File(target.getParentFile(), target.getName() + ".gz");
			if (targetFile.isFile()) {
				File renameTarget = new File(targetFile.getParentFile(),
						targetFile.getName() + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
				try {
					Files.move(targetFile.toPath(), renameTarget.toPath(), StandardCopyOption.REPLACE_EXISTING);
					logger.info(targetFile.getPath() + " renamed to " + renameTarget.getPath());
				} catch (IOException e) {
					throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, targetFile.getPath(), renameTarget.getPath(),
							e.getMessage());
				}
			}
	  	
			boolean closingReader = false;
			boolean closingWriter = false;
			try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(targetFile))) {
				try (FileInputStream in = new FileInputStream(source)) { 
					byte[] buff = new byte[2048];
			  	int bytesRead = -1;
			  	
					while ((bytesRead = in.read(buff, 0, 2048)) > 0) {
						out.write(buff, 0, bytesRead);
					}
					
					closingReader = true;
				} 
				
				closingWriter = true;
			} catch (IOException e) {
				if (!closingReader && !closingWriter) {
					throw new EDAGIOException(EDAGIOException.CANNOT_GZIP_FILE, source.getPath(), targetFile.getPath(), e.getMessage());
				} else {
					logger.warn("Unable to close input stream of " + source.getPath() + " or output stream of " + targetFile.getPath()
							+ ": " + e.getMessage());
				}
			}
			
			logger.info(targetFile.getPath() + " created");
			
			if (deleteSourceFile) {

				File file01 = new File(source.getParent() + "/" + procModel.getSrcInfo().getSourceFileName() + "_01");
				File file02 = new File(source.getParent() + "/" + procModel.getSrcInfo().getSourceFileName() + "_02");

				if (file01.delete()) {
					logger.info(file01.getPath() + " deleted");
				} else {
					logger.warn(file01.getPath() + " cannot be deleted, its file handle might be held by another process");
				}
				
				if (file02.delete()) {
					logger.info(file02.getPath() + " deleted");
				} else {
					logger.warn(file02.getPath() + " cannot be deleted, its file handle might be held by another process");
				}
			}
		}
	}
