package com.uob.edag.utils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

import com.uob.edag.exception.EDAGIOException;

/**
 * @Author : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description : The class contains the linux utility methods.
 * 
 */
public class LinuxUtils {
  private static Logger logger = Logger.getLogger(LinuxUtils.class);

  /**
   * This method is used to move a file from one location to another location.
   * @param srcfilePath The Path of the Source File to be moved
   * @param destFilePath The Destination Location where the Source file is to be moved
   * @throws Exception when there is an error moving the file to the destination
   */
  public static void moveFile(String srcfilePath, String destFilePath) throws EDAGIOException {
    File destFile = new File(destFilePath);
    if (destFile.exists()) {
    	logger.debug(destFile.getPath() + " exists, needs to be renamed first");
      long now = System.currentTimeMillis();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
      String newDestFile = destFile + "." + sdf.format(now);

      boolean renamed = destFile.renameTo(new File(newDestFile));
      if (!renamed) {
        throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, destFile.getPath(), newDestFile, "renameTo operation returns false");
      }
      logger.debug("Existing " + destFile.getPath() + " renamed to " + newDestFile);
    }
    
		try {
			org.apache.commons.io.FileUtils.moveFile(new File(srcfilePath), new File(destFilePath));
			logger.debug(srcfilePath + " moved to " + destFilePath);
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, srcfilePath, destFilePath, e.getMessage());
		}
  }
}
