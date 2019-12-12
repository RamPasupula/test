package com.uob.edag.model;

import java.io.StringWriter;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.context.Context;

import com.uob.edag.utils.VelocityUtils;

/**
 * @Author      : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description   : The file defines the parameter name and values which can be 
 *                  overridden for any process
 * 
 */
public class ProcessParam {
	
	protected Logger logger = Logger.getLogger(getClass());
	
  private String paramName;      // Name of the Parameter
  private String paramValue;        // Value of the Parameter
  
  @Override
  public String toString() {
    return new ToStringBuilder(this).append("paramName", paramName)
    		                            .append("paramValue", paramValue)
    		                            .toString();
  }
  
  public String getParamName() {
    return paramName;
  }
  
  public void setParamName(String paramName) {
    this.paramName = StringUtils.trimToEmpty(paramName);
  }
  
  public String getParamValue(ProcessModel processModel, ProcessInstanceModel procInstanceModel, Map<String, String> additionalParams) {
  	if (paramValue == null) {
  		return null;
  	}
  	
  	Context ctx = new VelocityContext();
  	if (processModel != null) {
  		if (processModel.getSrcInfo() != null) {
  			ctx.put("sourceFileName", processModel.getSrcInfo().getSourceFileName());
  		}
    	ctx.put("processID", processModel.getProcId());
    	ctx.put("sourceSystemCode", processModel.getSrcSysCd());
  	}
  	
  	if (procInstanceModel != null) {
			ctx.put("countryCode", procInstanceModel.getCountryCd());
			ctx.put("businessDate", procInstanceModel.getBizDate());
  	}
  	
  	if (additionalParams != null) {
	  	for (Entry<String, String> mapEntry : additionalParams.entrySet()) {
	  		ctx.put(mapEntry.getKey(), mapEntry.getValue());
	  	}
  	}
  	
  	StringWriter out = new StringWriter();
  	if (VelocityUtils.evaluate(ctx, out, "Evaluating parameter value for parameter " + paramName, paramValue)) {
  		String result = out.toString();
  		logger.debug(paramValue + " from " + procInstanceModel.getProcId() + "." + paramName + " parameter evaluated to " + result);
  		return result;
  	} else {
  		logger.warn("Unable to evaluate " + paramValue + " from " + procInstanceModel.getProcId() + "." + paramName + " parameter, returning parameter value as-is");
  		return paramValue;
  	}
  }
  
  public String getParamValue() {
    return paramValue;
  }
  
  public void setParamValue(String paramValue) {
    this.paramValue = paramValue;
  }
}
