package com.uob.edag.validation;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.FileModel;

public class InterfaceSpecHandlerTest {
	
	private InterfaceSpecHandler instance;

	@Before
	public void setUp() throws Exception {
		instance = new InterfaceSpecHandler();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testValidFileLayout() throws EDAGValidationException {
		FileModel srcInfo = new FileModel();
		instance.setFileLayout("PQ", srcInfo);
		assertEquals("|", srcInfo.getColumnDelimiter());
		assertEquals("\"", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("T", srcInfo);
		assertEquals("\\t", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("B", srcInfo);
		assertEquals("\\u0007", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("P", srcInfo);
		assertEquals("|", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("F", srcInfo);
		assertEquals("", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("FXD", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("CS", srcInfo);
		assertEquals(",", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("|~|", srcInfo);
		assertEquals("|~|", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
		
		instance.setFileLayout("~", srcInfo);
		assertEquals("~", srcInfo.getColumnDelimiter());
		assertEquals("", srcInfo.getTextDelimiter());
		assertEquals("DEL", srcInfo.getSourceFileLayoutCd());
	}
}
