package com.uob.edag.utils;

import java.util.List;
import java.util.Map;

import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.model.ControlModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessModel;

public interface FileUtility {

	enum OperationType{Local, Remote};
	
	void moveFile(String source, String target) throws EDAGException;
	void copyFile(String source, String target) throws EDAGException;
	boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel,
								   HadoopModel destInfo, String charsetName, boolean isSkipErrRecordsDisabled, Map<String, String> charReplacementMap) throws EDAGException;
	boolean removeControlCharacter(String fileName, String outputFile, FileModel sourceInfo, ControlModel controlModel,
                                   HadoopModel hadoopModel, String charsetName, boolean isSkipErrRecordsDisabled, int linesToValidate) throws EDAGException;
	List<String> readNLines(String fileName, int numLines, String charsetName) throws EDAGException;
	List<String> readFile(String fileName, String charsetName) throws EDAGException;
	void deleteFile(String fileName) throws EDAGException;
	void archiveFile(String sourceFileLoc, String archiveFileLoc, boolean deleteSourceFile) throws EDAGException;
	String readLineByIndicator(String filename, String lineInd, String charset) throws EDAGException;
	String[] removeHeaderAndFooter(String fileName, String bizDate, String charsetName) throws EDAGException;
	public void archiveFileOSM(ProcessModel procModel, String targetFilename, boolean deleteSourceFile) throws EDAGIOException;
}
