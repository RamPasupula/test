/**
 * 
 */
package com.uob.edag.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 * @author kg186041
 *
 */
public class OSMMergerUtil {
	
	private static Logger logger = Logger.getLogger(OSMMergerUtil.class);
	private static final String HEADER_TAG = "H|";
	private static final String FOOTER_TAG = "T|";
	private static final String NEXT_LINE = "\r\n";
	
	/**
	 * 
	 */
	public OSMMergerUtil() {
		// TODO Auto-generated constructor stub
	}
	
	
	private List<String> parseContent(List<String> contents) {
 		List<String> parseContent = new ArrayList<>();
		String header = null;
		int footerNum1 =0;
		int footerNum2=0;
		String footerHasum = null;
		Boolean hashSumFlag = false;
		int totalRow = 0;
		
		logger.debug("** load content is staring...");
		
		for (String strLine: contents) {
			if (strLine.startsWith(HEADER_TAG)) {
				header = strLine;
 		 	}
			if (strLine.startsWith(FOOTER_TAG)) {
				
				String[] footerArray = strLine.split("\\|");
				int firstPipe = strLine.indexOf("|");
				int secondPipe = strLine.substring(firstPipe + 1, strLine.length()).indexOf("|");
				if (secondPipe == -1 ) {
					int rowNum = Integer.valueOf(strLine.substring(strLine.indexOf('|') + 1, strLine.length()));
					totalRow += rowNum;
				}
				
				if (secondPipe > -1) {
					hashSumFlag = true;
					footerNum1 = footerNum1 + Integer.valueOf(footerArray[1]);

					if (footerArray.length == 2) {
						footerHasum = "";
					}else {
						footerNum2 = footerNum2 + Integer.valueOf(footerArray[2]);
						footerHasum = String.valueOf(footerNum2);
					}
				}
 		 	}
			
			if (strLine.startsWith(HEADER_TAG) || strLine.startsWith(FOOTER_TAG))
				 continue;
		 	parseContent.add(strLine);
		}
 	 
 		parseContent.add(0, header);
 		if (hashSumFlag) {
 	 		parseContent.add(FOOTER_TAG.concat(String.valueOf(footerNum1)).concat("|").concat(footerHasum));
 		} else {
 	 		parseContent.add(FOOTER_TAG.concat(String.valueOf(totalRow)));
 		}

		return parseContent;
	}
	
	private List<String> readLargeFile(String fileName) throws IOException {
	    List<String> strLines = new ArrayList<>();
		Path path = Paths.get(fileName);
	    try (BufferedReader reader = Files.newBufferedReader(path)){
	      String line = null;
	      while ((line = reader.readLine()) != null) {
 	    	  strLines.add(line);
	      }      
	    }	
	   return strLines; 
	}
	
	private void writeToLargeFile(String fileName, List<String> contents) throws IOException {
 	    Path path = Paths.get(fileName);
	    try (BufferedWriter writer = Files.newBufferedWriter(path)){
	      for(String line : contents){
	        writer.write(line);
	        writer.newLine();
	      }
	    }
	}
	
	public String mergeFileBuffer(String fileName, String location) {
		
		String outputFile = null;
 	 	try  {
 			
 			File finalFile = new File(location + File.separator + fileName);
 			
 			 File dir = new File(location);
 			 File[] srcFiles = dir.listFiles();
 			 List<File> filesToMerge = new ArrayList<File>();
			 for (File srcFile: srcFiles) {
				 //if (srcFile.isFile() && srcFile.getName().startsWith(fileName) && !srcFile.getName().equalsIgnoreCase(fileName)) {
				 if (srcFile.isFile() 
						 && (srcFile.getName().equalsIgnoreCase(fileName+"_01") || srcFile.getName().equalsIgnoreCase(fileName+"_02")) 
						 && !srcFile.getName().equalsIgnoreCase(fileName)) {
					 logger.debug(srcFile.getName());
					 filesToMerge.add(srcFile);
				 }
			 }
 			
 			if (finalFile.exists()) {// && filesToMerge.size() !=0
 				FileUtils.deleteQuietly(finalFile);
 			}
 			
 		 	 /*
 			  * Get All Valid Files
 			  * */ 
 			
			 
			 List<String> mergeContent = new ArrayList<>();
		 	 for (File srcFile: filesToMerge) {
		 		 
		 		 List<String> srcContents = this.readLargeFile(srcFile.getAbsolutePath());
		 		mergeContent.addAll(srcContents);
		 	 }
		 	 
		 	 List<String> parseContents = this.parseContent(mergeContent);
		 	 
	 		 this.writeToLargeFile(finalFile.getAbsolutePath(), parseContents);
	 		 
			 outputFile = finalFile.getAbsolutePath();
			 
			 
//			 for (File singleFile: filesToMerge) {
//				 singleFile.delete();
//			 }
			 
 	 	} catch (Exception e) {
			logger.error("ERROR: File merge failed >> ", e);
			outputFile = null;
		}
		
		return outputFile;
	}
	
	public String mergeFile(String fileName, String location) {
		String outputFile = null;
 		try  {
 			
 			File finalFile = new File(location + File.separator + fileName);
 			if (finalFile.exists()) {
 				FileUtils.deleteQuietly(finalFile);
 			}
 		 	 /*
 			  * Get All Valid Files
 			  * */ 
 			 File dir = new File(location);
 			 File[] srcFiles = dir.listFiles();
 			 List<File> filesToMerge = new ArrayList<File>();
			 for (File srcFile: srcFiles) {
				 if (srcFile.isFile()
						 && srcFile.getName().startsWith(fileName)) {
					 logger.debug(srcFile.getName());
					 filesToMerge.add(srcFile);
				 }
			 }
			 
			 /*
			  * 
			  * */
			 File mergeFile = File.createTempFile(fileName, "");
			 
			 StringBuilder sbContent = new StringBuilder();
			 StringBuilder sbComplete = new StringBuilder();
			 int totalSize = 0;
			 String header = "";
			 for (File srcFile: filesToMerge) {
				 List<String> strContents = FileUtils.readLines(srcFile, Charset.defaultCharset());
				 if (!strContents.isEmpty()) {
					 int strSize = (strContents.size() - 2); // Minus HEADER & FOOTER Line
					 header = strContents.get(0) != null ? strContents.get(0).concat(NEXT_LINE): "";
					  for (String strLine: strContents) {
						 if (strLine.startsWith(HEADER_TAG) || strLine.startsWith(FOOTER_TAG))
							 continue;
						 
						 sbContent.append(strLine).append(NEXT_LINE);
					 }
					 totalSize += strSize;
				 }
		 	 }
			  
			 sbComplete.append(header).append(sbContent).append(FOOTER_TAG).append(totalSize);
			 logger.debug(sbComplete.toString());
			 FileUtils.writeStringToFile(mergeFile, sbComplete.toString(), Charset.defaultCharset());
			 
			 boolean success = mergeFile.renameTo(finalFile);
			 if (success)   
			 outputFile = mergeFile.getAbsolutePath();
			 
 	 	} catch (Exception e) {
			logger.error("ERROR: File merge failed >> ", e);
			outputFile = null;
		}
		
		return outputFile;
	}
	/** 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		BasicConfigurator.configure();
		OSMMergerUtil mergerUtil = new OSMMergerUtil();
		String output = mergerUtil.mergeFileBuffer("AL_APPLICATION", "C:\\Users\\TW186013\\Desktop\\OSM");
		System.out.println("output: " + output);
	}

}
