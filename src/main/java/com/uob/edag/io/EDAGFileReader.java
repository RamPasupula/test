package com.uob.edag.io;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

public class EDAGFileReader extends LineNumberReader {
	
	public static final int DEFAULT_BUFFER_SIZE = 8 * 1024 * 1024;
	public static final Pattern NEW_LINE_PATTERN = Pattern.compile("\\n|\\r\\n");
	public static final String NEW_LINE = "\n";
	public static final char CARRIAGE_RETURN = '\r';
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private String footerLine = null;
	private StringBuilder buff = new StringBuilder();
	private int lineNo = 0;
	private Map<Pattern, String> controlCharMap;
	private char[] b;
	
	public String toString() {
		return new ToStringBuilder(this).append("lineNo", lineNo)
				                            .append("controlCharMap", controlCharMap)
				                            .append("footerLine", footerLine)
				                            .toString();
	}
	
	public EDAGFileReader(Reader in, String footerLine) {
		this(in, DEFAULT_BUFFER_SIZE, footerLine);
	}
	
	public EDAGFileReader(Reader in, int sz, String footerLine) {
		this(in, sz, footerLine, null);
	}
	
	public EDAGFileReader(Reader in, String footerLine, Map<Pattern, String> controlCharMap) {
		this(in, DEFAULT_BUFFER_SIZE, footerLine, controlCharMap);
	}
	
	public EDAGFileReader(Reader in, int sz, String footerLine, Map<Pattern, String> controlCharMap) {
		super(in, sz);
		this.b = new char[sz];
		this.footerLine = footerLine;
		this.controlCharMap = controlCharMap;
		logger.debug(toString());
	}
	
	public String readLine() throws IOException {
		String line = internalReadLine();
		
		if (lineNo == 1 && StringUtils.isNotBlank(footerLine)) { // if footer is not blank it means 1st line is header, so it should be skipped
			line = internalReadLine();
		}
		
		if (StringUtils.equals(line, footerLine)) {
			line = null;
		}
		
		return line;
	}
	
	protected String internalReadLine() throws IOException {
		String result = null;
		
		boolean hasNewLine = false;
		int charsRead = -1;
		int newLineIndex = -1;
		int copyToIndex = -1;
		do {
		  // first we search for line in buffer
			// Matcher matcher = NEW_LINE_PATTERN.matcher(buff);
			// hasNewLine = buff.length() > 0 ? matcher.find() : false;
			hasNewLine = ((newLineIndex = buff.indexOf(NEW_LINE)) >= 0);
			
			if (!hasNewLine) {
				// new line char(s) is not found in buffer, read more from file
				charsRead = super.read(b, 0, b.length);
				if (charsRead > 0) {
					// add characters read to buffer
					buff.append(b, 0, charsRead);
				}
			}	else {
				// see if the char prior to \n is \r
				copyToIndex = (newLineIndex > 0 && buff.charAt(newLineIndex - 1) == CARRIAGE_RETURN) ? newLineIndex - 1 : newLineIndex;
				
				// we found new line char(s), get string from beginning of buffer to start of new line char(s)
				lineNo++;
				result = buff.substring(0, copyToIndex);
				
				// remove characters from the beginning of the buffer to the end of the new line char(s) 
				buff.delete(0, newLineIndex + 1);
			}
 		} while (!hasNewLine && charsRead > 0);
		
		if (!hasNewLine && buff.length() > 0) {
			// last line in the file
			result = buff.toString();
			lineNo++;
		  buff.setLength(0);
		}
		
		if (result != null && controlCharMap != null) {
			for (Entry<Pattern, String> patternEntry : controlCharMap.entrySet()) {
				Pattern pattern = patternEntry.getKey();
				String replacement = patternEntry.getValue();
				Matcher matcher = pattern.matcher(result);
				result = matcher.replaceAll(replacement);
			}
		}
		
		return result;
	}
	
	public void setLineNumber(int lineNo) {
		this.lineNo = lineNo;
	}
	
	public int getLineNumber() {
		return this.lineNo;
	}
}
