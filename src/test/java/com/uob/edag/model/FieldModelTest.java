package com.uob.edag.model;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.uob.edag.exception.EDAGValidationException;

public class FieldModelTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testClone() {
		FieldModel model = new FieldModel();
		model.setDataFormat("data format 1");
		model.setDataType("data type 2");
		model.setDecimalIndicator(",");
		model.setDecimalPrecision(3);
		model.setDefaultValue("this is the default value");
		model.setEndPosition(1000);
		model.setOptionality("M");
		model.setFieldDesc("this is the field description");
		model.setFieldName("this is the field name");
		model.setFieldNum(15);
		model.setFieldValue("this is the field value");
		model.setFileId(150);
		model.setHashSumField(true);
		model.setIndexField(true);
		model.setLength(120);
		model.setProfileField(true);
		try {
			model.setRecordTypeInd("this is the record type");
		} catch (EDAGValidationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<Integer> rulesList = new ArrayList<Integer>();
		rulesList.add(123);
		rulesList.add(456);
		rulesList.add(789);
		model.setRulesList(rulesList);
		model.setStartPosition(500);
		model.setUserNm("this is the user name");
		
		FieldModel clone = model.clone();
		assertEquals(clone, model);
		assertEquals(clone.hashCode(), model.hashCode());
	}
}
