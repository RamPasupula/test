package com.uob.edag.exception;

import java.text.Format;
import java.text.MessageFormat;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.uob.edag.utils.PropertyLoader;

public class EDAGException extends Exception {
	
	public static final int DEFAULT_TRUNCATED_MESSAGE_MAX_LENGTH = 2000;
	
	public static class PredefinedException {
		
		private String name;
		private String messagePattern;
		private Format messageFormat;

		public PredefinedException(Class<?> owner, String exceptionCode, String defaultMessagePattern) {
			this.name = owner.getName() + "." + exceptionCode;
			String patternFromProperties = StringUtils.trimToNull(PropertyLoader.getProperty(name)); 
			messagePattern = patternFromProperties != null ? patternFromProperties : defaultMessagePattern;
			messageFormat = messagePattern != null ? new MessageFormat(messagePattern) : new MessageFormat(defaultMessagePattern);
		}
		
		public String getName() {
			return name;
		}
		
		public String getMessagePattern() {
			return messagePattern;
		}
		
		protected String getMessage(Object... params) {
			return messageFormat.format(params);
		}
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2751400418967878547L;
	
	public EDAGException(PredefinedException ex, Object... params) {
		this(ex.getMessage(params));
	}
	
	public EDAGException(String message) {
		super(StringUtils.trimToEmpty(message));
		log(this);
	}

	public EDAGException(String message, Throwable cause) {
		super(StringUtils.trimToEmpty(message), cause);
		log(this);
	}
	
	public EDAGException(PredefinedException ex, Throwable cause, Object... params) {
		this(ex.getMessage(params), cause);
	}

	public String getTruncatedMessage() {
		return getTruncatedMessage(DEFAULT_TRUNCATED_MESSAGE_MAX_LENGTH);
	}
	
	public String getTruncatedMessage(int maxLength) {
		String msg = this.getMessage();
		return msg.length() > maxLength ? msg.substring(0, maxLength - 1) : msg; 
	}
	
	public void log(Exception ex) {
		StackTraceElement[] stackTraces = ex.getStackTrace();
		Logger logger = stackTraces.length > 0 ? Logger.getLogger(stackTraces[0].getClassName()) 
				                                   : Logger.getLogger(ex.getClass());
		StringBuilder msg = new StringBuilder(ex.getMessage());
		
		Throwable cause = ex.getCause();
		if (cause instanceof NullPointerException) {
			for (StackTraceElement el : cause.getStackTrace()) {
				msg.append(System.lineSeparator() + "  " + el);
			}
		}
		
		logger.error(msg.toString());
	}
}
