package com.uob.edag.model;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ControlModelTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testClone() {
		ControlModel model = new ControlModel();
		model.setBizDate("2017-05-12");
		model.setCtryCd("SG");
		model.setFileName("test.txt");
		model.setHashSumCol("col1");
		model.setHashSumVal("hash sum value");
		model.setHashSumValTarget("hash sum value target");
		model.setSrcSystemCd("system123");
		model.setSysDate("2017-05-13");
		model.setTotalErrRecordsTarget(123);
		model.setTotalRecords(16000);
		model.setTotalRecordsTarget(16123);
		
		ControlModel clone = model.clone();
		assertEquals(clone, model);
		assertEquals(clone.hashCode(), model.hashCode());
	}
}
