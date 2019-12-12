package com.uob.edag.utils;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.apache.log4j.Logger;
import org.junit.Test;

public class UobUtilsTest {
    static Logger logger = null;

  @Test
  public void removeHeaderAndFooterFromFile() throws Exception {
      logger = Logger.getLogger(UobUtils.class);
      RemoteFileUtils sshConn = new RemoteFileUtils("NODE1");
	  sshConn.removeHeaderAndFooter("/Users/cb186046/IdeaProjects/edf/data/test/file_w_hder_fter", "2017-02-02", Charset.defaultCharset().name());
  }

  @Test
  public void parseBoolean1() throws Exception {
	boolean output = UobUtils.parseBoolean("Y");
	assertEquals(output, true);
  }

  @Test
  public void parseBoolean2() throws Exception {
	boolean output = UobUtils.parseBoolean("true");
	assertEquals(output, true);
  }

  @Test
  public void parseBoolean3() throws Exception {
	boolean output = UobUtils.parseBoolean("1");
	assertEquals(output, true);
  }

  @Test
  public void parseBoolean4() throws Exception {
	boolean output = UobUtils.parseBoolean("N");
	assertEquals(output, false);
  }

  @Test
  public void parseBoolean5() throws Exception {
	boolean output = UobUtils.parseBoolean("false");
	assertEquals(output, false);
  }

  @Test
  public void parseBoolean6() throws Exception {
	boolean output = UobUtils.parseBoolean("0");
	assertEquals(output, false);
  }

  @Test
  public void parseBoolean7() throws Exception {
	boolean output = UobUtils.parseBoolean("");
	assertEquals(output, false);
  }

  @Test(expected = Exception.class)
  public void parseBoolean8() throws Exception {
	UobUtils.parseBoolean("NonBoolean");
  }

  @Test
  public void checkHiveReservedKeyword1() {
	boolean output = UobUtils.checkHiveReservedKeyword("EXTENDED");
	assertEquals(output, true);
  }

  @Test
  public void checkHiveReservedKeyword2() {
	boolean output = UobUtils.checkHiveReservedKeyword("EDAG");
	assertEquals(output, false);
  }
}
