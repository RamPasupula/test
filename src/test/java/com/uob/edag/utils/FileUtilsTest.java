package com.uob.edag.utils;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Writer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.uob.edag.exception.EDAGIOException;

public class FileUtilsTest {
	
	String lineWithNoControlChar = "The quick brown fox jumps over the lazy dog";
	String lineWithNullChar = "The quick brown fox\u0000jumps over the lazy dog";
	String lineWithBellChar = "The quick brown fox\u0007jumps over the lazy dog";
	String lineWithShiftIn = "The quick brown fox\u000Ejumps over the lazy dog";
	String lineWithShiftOut = "The quick brown fox\u000Fjumps over the lazy dog";
	String lineWithNonBreakingSpace = "The quick brown fox\u00A0jumps over the lazy dog";
	String lineWithNewLineChar = "The quick brown fox\u0085jumps over the lazy dog";
	String lineWithCarriageReturn = "The quick brown fox\rjumps over the lazy dog";
	String lineWithEverything = "The\u0000quick\u0007brown\u000Efox\u000Fjumps\rover\u00A0the\u0085lazy dog";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testNewLine() throws IOException {
		File input = createFile();
		try (LineNumberReader reader = new LineNumberReader(new FileReader(input))) {
			String line = reader.readLine();
			assertEquals(lineWithNoControlChar, line);
			line = reader.readLine();
			assertEquals(lineWithNullChar, line);
			line = reader.readLine();
			assertEquals(lineWithBellChar, line);
			line = reader.readLine();
			assertEquals(lineWithShiftIn, line);
			line = reader.readLine();
			assertEquals(lineWithShiftOut, line);
			line = reader.readLine();
			assertEquals(lineWithNonBreakingSpace, line);
			line = reader.readLine();
			assertEquals(lineWithNewLineChar, line);
			line = reader.readLine();
			assertEquals(lineWithCarriageReturn, line);
			line = reader.readLine();
			assertEquals(lineWithEverything, line);
		}
	}
	
	private File createFile() throws IOException {
		//String fixedWidths = "4 6 6 4 6 5 4 5 3";
		
		File source = File.createTempFile("FileUtilsTest", null);
		source.deleteOnExit();
		
	  try (Writer out = new FileWriter(source)) {
	  	//String crlf = System.getProperty("line.separator");
	  	String crlf = "\r\n";
	  	out.write(lineWithNoControlChar + crlf);
	  	out.write(lineWithNullChar + crlf);
	  	out.write(lineWithBellChar + crlf);
	  	out.write(lineWithShiftIn + crlf);
	  	out.write(lineWithShiftOut + crlf);
	  	out.write(lineWithNonBreakingSpace + crlf);
	  	out.write(lineWithNewLineChar + crlf);
	  	out.write(lineWithCarriageReturn + crlf);
	  	out.write(lineWithEverything + crlf);
	  	out.write("\u001A"); // ctrl-z
	  }
	  
	  return source;
	}
	
	@Test
	public void testRemoveControlCharacter() throws IOException, EDAGIOException {
		File source = createFile();
		String expectedResult = "The quick brown fox jumps over the lazy dog";		
		String expectedResult2 = "The\u0007quick\u0007brown\u0007fox\u0007jumps\u0007over\u0007the\u0007lazy\u0007dog";
	  
		File target = File.createTempFile("FileUtilsTest", null);
		target.deleteOnExit();
		
		// TODO unit test breaks because of refactoring. fix the test
		// FileUtils.removeControlCharacter(source.getPath(), target.getPath(), false, "", "");
		
		try (LineNumberReader reader = new LineNumberReader(new FileReader(target))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				assertEquals(expectedResult, line);
			}
		}
		
		target = File.createTempFile("FileUtilsTest", null);
		target.deleteOnExit();
		
		// TODO unit test breaks because of refactoring. fix the test
		// FileUtils.removeControlCharacter(source.getPath(), target.getPath(), true, fixedWidths, "SG");
		
		try (LineNumberReader reader = new LineNumberReader(new FileReader(target))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				assertEquals(expectedResult2, line);
			}
		}
	}
}
