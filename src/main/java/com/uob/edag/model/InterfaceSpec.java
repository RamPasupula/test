package com.uob.edag.model;

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.uob.edag.utils.InterfaceSpecMap;

/**
 * @Author       : Daya Venkatesan.
 * @Date of Creation: 10/24/2016
 * @Description    : The file is the model for the Interface File Specification provided to the 
 *                   Bulk Registration Utility
 * 
 */

public class InterfaceSpec {
  public String getSrcSystem() {
    return srcSystem;
  }
  
  public void setSrcSystem(String srcSystem) {
    this.srcSystem = srcSystem;
  }
  
  public Map<String, InterfaceSpecMap> getSrcFileSpec() {
    return srcFileSpec;
  }
  
  public void setSrcFileSpec(Map<String, InterfaceSpecMap> srcFileSpec) {
    this.srcFileSpec = srcFileSpec;
  }
  
  public Map<String, Map<String, InterfaceSpecMap>> getSrcFieldSpec() {
    return srcFieldSpec;
  }
  
  public void setSrcFieldSpec(Map<String, Map<String, InterfaceSpecMap>> srcFieldSpec) {
    this.srcFieldSpec = srcFieldSpec;
  }

  public Map<String, Map<String, InterfaceSpecMap>> getProcessSpec() {
    return processSpec;
  }
  
  public void setProcessSpec(Map<String, Map<String, InterfaceSpecMap>> processSpec) {
    this.processSpec = processSpec;
  }
  
  public Map<String, Map<String, String>> getParamSpec() {
    return paramSpec;
  }

  public void setParamSpec(Map<String, Map<String, String>> paramSpec) {
    this.paramSpec = paramSpec;
  }

  public Map<String, String> getDownstreamSpec() {
    return downstreamSpec;
  }

  public void setDownstreamSpec(Map<String, String> downstreamSpec) {
    this.downstreamSpec = downstreamSpec;
  }
  
  public InterfaceSpecMap getCtrlFileSpec() {
    return ctrlFileSpec;
  }

  public void setCtrlFileSpec(InterfaceSpecMap ctrlFileSpec) {
    this.ctrlFileSpec = ctrlFileSpec;
  }

  public Map<String, InterfaceSpecMap> getCtrlFieldSpec() {
    return ctrlFieldSpec;
  }

  public void setCtrlFieldSpec(Map<String, InterfaceSpecMap> ctrlFieldSpec) {
    this.ctrlFieldSpec = ctrlFieldSpec;
  }

  public String getInterfaceSpecName() {
    return interfaceSpecName;
  }

  public void setInterfaceSpecName(String interfaceSpecName) {
    this.interfaceSpecName = interfaceSpecName;
  }
  
  public Map<String, InterfaceSpecMap> getExportProcessSpec() {
    return exportProcessSpec;
  }

  public void setExportProcessSpec(Map<String, InterfaceSpecMap> exportProcessSpec) {
    this.exportProcessSpec = exportProcessSpec;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("srcSystem", srcSystem)
    		                            .append("interfaceSpecName", interfaceSpecName)
                                    .append("processSpec", processSpec)
                                    .append("exportProcessSpec", exportProcessSpec)
                                    .append("paramSpec", paramSpec)
                                    .append("downstreamSpec", downstreamSpec)
                                    .append("srcFileSpec", srcFileSpec)
                                    .append("ctrlFileSpec", ctrlFileSpec)
                                    .append("srcFieldSpec", srcFieldSpec)
                                    .append("ctrlFieldSpec", ctrlFieldSpec)
                                    .toString();
  }
  
  private String srcSystem;                            // Source System of the Interface Spec File
  private String interfaceSpecName;
  //Process Specifications
  private Map<String, Map<String, InterfaceSpecMap>> processSpec; 
  private Map<String, InterfaceSpecMap> exportProcessSpec;
  private Map<String, Map<String, String>> paramSpec; 
  private Map<String, String> downstreamSpec; 
  //Source File Specifications
  private Map<String, InterfaceSpecMap> srcFileSpec;    
  private InterfaceSpecMap ctrlFileSpec;
  //Source Field Specifications
  private Map<String, Map<String, InterfaceSpecMap>> srcFieldSpec;
  private Map<String, InterfaceSpecMap> ctrlFieldSpec;
}
