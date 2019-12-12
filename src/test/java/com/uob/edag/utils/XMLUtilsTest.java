package com.uob.edag.utils;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;

import com.uob.edag.exception.EDAGXMLException;

public class XMLUtilsTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testEvaluateXPath() throws IOException, EDAGXMLException {
		try (InputStream in = this.getClass().getResourceAsStream("FI_LNS_LNTIER_D01_SG_2018-05-18.xml")) {
			Document doc = XMLUtils.parseDocument(in);
			String sourceFileDir = XMLUtils.evaluateXPath("/root/project/mapping/parameter[@name='source_file_dir']", doc);
			assertEquals("/data/d01/cdtransfer/edasg/lnssg/processing", sourceFileDir);
		}
	}
	
	@Test
	public void testEscape() {
		String escaped = XMLUtils.escape("\"The quick brown <fox> jumps over the lazy 'dog' && 'cat'\"");
		assertEquals("&quot;The quick brown &lt;fox&gt; jumps over the lazy &apos;dog&apos; &amp;&amp; &apos;cat&apos;&quot;", escaped);
	}
}
