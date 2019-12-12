package com.uob.edag.runtime;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.Charset;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

public class ReplaceStepTest {

	public static void main(String[] args) throws UnsupportedEncodingException, DecoderException {
		ReplaceStep replacer = new ReplaceStep();
		replacer.setReplace_pattern("07,00,0E,0F,A0,0D,85");
		replacer.setReplace("20");
    System.out.println(args[0]);
    System.out.println(new BigInteger(1,args[0].getBytes("ISO-8859-1")));
    String formatted = String.format("%040x", new BigInteger(1,args[0].getBytes("ISO-8859-1")));
    System.out.println(formatted);
    
    byte[] bytes = Hex.decodeHex(formatted.toCharArray());
    String actualRecord = new String(bytes, Charset.defaultCharset());
    System.out.println(actualRecord);
    
    String replaced = replacer.performStep(args[0]);
    bytes = Hex.decodeHex(replaced.toCharArray());
    actualRecord = new String(bytes, Charset.defaultCharset());
		System.out.println(actualRecord);
	}
}
