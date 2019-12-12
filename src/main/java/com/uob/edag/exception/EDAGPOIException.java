package com.uob.edag.exception;

public class EDAGPOIException extends EDAGIOException {
	
	public static final PredefinedException CANNOT_OPEN_EXCEL_WORKBOOK = new PredefinedException(EDAGPOIException.class, "CANNOT_OPEN_EXCEL_WORKBOOK", "Unable to open Excel workbook {0}: {1}");

	/**
	 * 
	 */
	private static final long serialVersionUID = -4240811999016476029L;
	public EDAGPOIException(String message) {
		super(message);
	}

	public EDAGPOIException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGPOIException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGPOIException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}
