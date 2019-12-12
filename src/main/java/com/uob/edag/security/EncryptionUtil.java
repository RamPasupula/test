package com.uob.edag.security;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.uob.edag.exception.EDAGSecurityException;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description : The file is used for encrypting and decrypting sensitive data.
 * 
 */

public class EncryptionUtil {
	
	protected Logger logger = Logger.getLogger(getClass());

  private static final char[] PASSWORD = "enfldsgbnlsngdlksdsgm".toCharArray();
  private static final byte[] SALT = {(byte) 0xde, (byte) 0x33, (byte) 0x10, (byte) 0x12, 
                                      (byte) 0xde, (byte) 0x33, (byte) 0x10, (byte) 0x12,};
  private static final Charset UTF_8 = Charset.forName("UTF-8");

	public static final String CANNOT_CREATE_CIPHER = "CANNOT_CREATE_CIPHER";
	public static final String ENCODING_NOT_SUPPORTED = "ENCODING_NOT_SUPPORTED";

  /**
   * This method is used to run the Encryption util to create the Encrypted password.
   * @param args The text to be encrypted
   * @throws EDAGSecurityException 
   * @throws Exception when there is error in the encryption process
   */
  public static void main(String[] args) throws EDAGSecurityException {
  	String input = args[0];
  	boolean encrypt = true;
  	if (args.length > 1) {
  		encrypt = ("encrypt".equalsIgnoreCase(args[1]));
  	}

    EncryptionUtil util = new EncryptionUtil();
    String output = encrypt ? util.encrypt(input) : util.decrypt(input);
    System.out.println((encrypt ? "Encrypted input: " : "Decrypted input: ") + output);
  }

  /**
   * This method is used to do the encryption of the given input string.
   * @param property the string to be encrypted
   * @return This method returns the encrypted string.
   * @throws GeneralSecurityException when there is an error in the Encryption
   * @throws UnsupportedEncodingException when there is an error in the encoding
   */
  public String encrypt(String property) throws EDAGSecurityException {
  	try {
	    return base64Encode(createCipher(true).doFinal(property.getBytes(UTF_8)));
  	} catch (IllegalBlockSizeException | BadPaddingException e) {
  		throw new EDAGSecurityException(EDAGSecurityException.CANNOT_ENCRYPT, e.getMessage());
  	} 
  }
  
  private Cipher createCipher(boolean forEncryption) throws EDAGSecurityException {
		try {
			SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
			logger.debug("Secret key factory created");
			
	    SecretKey key = keyFactory.generateSecret(new PBEKeySpec(PASSWORD));
	    logger.debug("Secret key created");
	    
	    Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
	    logger.debug("Cipher created");
	    
	    pbeCipher.init((forEncryption ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE), key, new PBEParameterSpec(SALT, 20));
	    logger.debug("Cipher initialized");
	    
	    return pbeCipher;
		} catch (NoSuchAlgorithmException | InvalidKeySpecException | NoSuchPaddingException | InvalidKeyException | 
				     InvalidAlgorithmParameterException e) {
			throw new EDAGSecurityException(EDAGSecurityException.CANNOT_CREATE_CIPHER, e.getMessage());
		}
  }

  private String base64Encode(byte[] bytes) {
    return new Base64().encodeToString(bytes);
  }

  /**
   * This method is used to decrypted an encrypted string back to text format.
   * @param property The encrypted string to be decrypted
   * @return This method returns the decrypted string
   * @throws GeneralSecurityException when there is an error in the decryption
   * @throws IOException when there is an error in the decryption process.
   */
  public String decrypt(String property) throws EDAGSecurityException {
    try {
			return new String(createCipher(false).doFinal(base64Decode(property)), UTF_8);
		} catch (IllegalBlockSizeException | BadPaddingException e) {
			throw new EDAGSecurityException(EDAGSecurityException.CANNOT_DECRYPT, e.getMessage());
		} 
  }

  private byte[] base64Decode(String property) {
    return new Base64().decode(property);
  }
}