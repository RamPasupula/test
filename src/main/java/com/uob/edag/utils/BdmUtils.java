package com.uob.edag.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;

import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGXMLException;

public class BdmUtils {
	
	public enum CreationMethod {VELOCITY, DOM};
	
	protected Logger logger = Logger.getLogger(getClass());
	
	public void createBdmParamFileFromTemplate(String templateFileName, String targetFilePath, Map<String, String> dataMap) throws EDAGIOException, EDAGXMLException {
		createBdmParamFileFromTemplate(templateFileName, targetFilePath, dataMap, CreationMethod.VELOCITY);
	}
	
	public void createBdmParamFileFromTemplate(String templateFileName, String targetFilePath, Map<String, String> dataMap, CreationMethod method) throws EDAGIOException, EDAGXMLException {
		if (CreationMethod.DOM == method) {
			createBdmParamFileFromDOMTemplate(templateFileName, targetFilePath, dataMap); 
		} else {
			createBdmParamFileFromVelocityTemplate(templateFileName, targetFilePath, dataMap);
		}
	}
	
	private void createBdmParamFileFromVelocityTemplate(String templateFileName, String targetFilePath, Map<String, String> dataMap) throws EDAGIOException {
		VelocityContext ctx = new VelocityContext();
		if (dataMap != null) {
			for (String key : dataMap.keySet()) {
				ctx.put(key, XMLUtils.escape(dataMap.get(key)));
			}
		}
		
		boolean closingReader = false;
		try (Reader reader = new InputStreamReader(getClass().getResourceAsStream(templateFileName))) {
		  VelocityUtils.evaluate(ctx, new File(targetFilePath), "Creating " + targetFilePath + " BDM param file", reader, null);
			
			closingReader = true;
		} catch (IOException e) {
			if (closingReader) {
				logger.warn("Unable to read resource " + templateFileName);
			} else {
				throw new EDAGIOException(EDAGIOException.CANNOT_READ_RESOURCE, templateFileName, e.getMessage());
			}
		}
	}
	
	/**
   * This method is used to create the BDM Parameter XML file from the Parameter Template File.
   * @param templateFileName The path of the Parameter Template file
   * @param targetFilePath The target location where the parameter xml file has to be created
   * @param dataMap a key-value map with parameters to be replaced in the template file.
   * @throws Exception when there is an error creating the BDM Parameter XML file.
   */
  private void createBdmParamFileFromDOMTemplate(String templateFileName, String targetFilePath,
                                                 Map<String, String> dataMap) throws EDAGXMLException, EDAGIOException {
    File tempFile = null;
    File tempFileDir = new File(targetFilePath).getParentFile();
		try {
			tempFile = File.createTempFile("BDMTemplate", ".tmp", tempFileDir);
			logger.debug(tempFile.getPath() + " created as temporary file");
		} catch (IOException e1) {
			throw new EDAGIOException(EDAGIOException.CANNOT_CREATE_TEMP_FILE_IN_DIR, "BDMTemplate", ".tmp", tempFileDir.getPath(), e1.getMessage());
		}

    InputStream stream = getClass().getResourceAsStream(templateFileName);
    if (stream == null) {
    	throw new EDAGIOException(EDAGIOException.CANNOT_READ_RESOURCE, templateFileName, "Stream is null");
    }
    
    logger.debug(templateFileName + " opened as input stream");
    try {
			org.apache.commons.io.FileUtils.copyInputStreamToFile(stream, tempFile);
			logger.debug("Input stream written to " + tempFile.getPath());
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_WRITE_FILE, tempFile.getPath(), e.getMessage());
		}	

    Document doc = null;
		doc = XMLUtils.parseDocument(tempFile);
		logger.debug("DOM created from " + tempFile.getPath());
    doc.getDocumentElement().normalize();
    logger.debug("DOM normalized");

    NodeList mappingList = doc.getElementsByTagName("mapping");
    for (int m = 0; m < mappingList.getLength(); m++) {
      Node mapping = mappingList.item(m);
      if (mapping.getNodeType() == Node.ELEMENT_NODE) {
        Element mappingEl = (Element) mapping;
        
        // EDF-70
        String nameAttribute = mappingEl.getAttribute("name");
        if (nameAttribute.startsWith("${") && nameAttribute.endsWith("}")) {
        	String nameToken = nameAttribute.substring(2, nameAttribute.length() - 1);
        	String val = dataMap.get(nameToken);
        	mappingEl.setAttribute("name", val);
        	logger.debug(nameToken + " replaced with " + val);
        }
        
        NodeList parameterList = mappingEl.getElementsByTagName("parameter");
        for (int count = 0; count < parameterList.getLength(); count++) {
          Node parameter = parameterList.item(count);
          String parameterValue = parameter.getFirstChild().getNodeValue();
          if (parameterValue.startsWith("${") && parameterValue.endsWith("}")) {
            parameterValue = parameterValue.substring(2, parameterValue.length() - 1);
            String valToReplace = dataMap.get(parameterValue);
            parameter.getFirstChild().setNodeValue(valToReplace);
            logger.debug(parameterValue + " replaced with " + valToReplace);
          }
        }
      }
    }
  
    logger.debug("Going to create BDM param file in path: " + targetFilePath);
    XMLUtils.transform(new DOMSource(doc), new StreamResult(new File(targetFilePath)));

    logger.debug("BDM Param file created successfully");
    tempFile.delete();
  }
}
