package com.uob.edag.exception;

public class EDAGSecurityException extends EDAGException {
	
	public static final PredefinedException CANNOT_CREATE_CIPHER = new PredefinedException(EDAGSecurityException.class, "CANNOT_CREATE_CIPHER", "Cannot create cipher: {0}");
	public static final PredefinedException CANNOT_ENCRYPT = new PredefinedException(EDAGSecurityException.class, "CANNOT_ENCRYPT", "Unable to encrypt: {0}");
	public static final PredefinedException CANNOT_DECRYPT = new PredefinedException(EDAGSecurityException.class, "CANNOT_DECRYPT", "Unable to decrypt: {0}");
	public static final PredefinedException CANNOT_LOGIN_USING_KEYTAB = new PredefinedException(EDAGSecurityException.class, "CANNOT_LOGIN_USING_KEYTAB", "Unable to login user {0} using keytab file {1}: {2}");

	/**
	 * 
	 */
	private static final long serialVersionUID = 5916877384560465622L;
	
	public EDAGSecurityException(PredefinedException ex, Object... params) {
		super(ex, params);
	}

	public EDAGSecurityException(String message) {
		super(message);
	}

	public EDAGSecurityException(String message, Throwable cause) {
		super(message, cause);
	}

	public EDAGSecurityException(PredefinedException ex, Throwable cause, Object... params) {
		super(ex, cause, params);
	}
}