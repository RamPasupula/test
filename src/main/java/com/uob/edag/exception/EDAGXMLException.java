package com.uob.edag.exception;

public class EDAGXMLException extends EDAGException {
	
	public static final PredefinedException CANNOT_PARSE_INPUT_STREAM = new PredefinedException(EDAGXMLException.class, "CANNOT_PARSE_INPUT_STREAM", "Unable to parse DOM document from input stream: {0}");
	public static final PredefinedException CANNOT_PARSE_FILE = new PredefinedException(EDAGXMLException.class, "CANNOT_PARSE_FILE", "Unable to parse DOM document from file {0}: {1}");
	public static final PredefinedException CANNOT_TRANSFORM = new PredefinedException(EDAGXMLException.class, "CANNOT_TRANSFORM", "Unable to transform XML from {0} to {1}: {2}");
	public static final PredefinedException CANNOT_EVALUATE_XPATH = new PredefinedException(EDAGXMLException.class, "CANNOT_EVALUATE_XPATH", "Unable to evaluate XPath expression {0}: {1}");

	/**
	 * 
	 */
	private static final long serialVersionUID = 946706001304958972L;

	public EDAGXMLException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGXMLException(String message) {
		super(message);
	}

	public EDAGXMLException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGXMLException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
