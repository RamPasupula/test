package com.uob.edag.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CalendarModel {
  public String getSrcSysCd() {
    return srcSysCd;
  }
  
  public void setSrcSysCd(String srcSysCd) {
    this.srcSysCd = srcSysCd;
  }
  
  public String getCtryCd() {
    return ctryCd;
  }
  
  public void setCtryCd(String ctryCd) {
    this.ctryCd = ctryCd;
  }
  
  public String getFreqCd() {
    return freqCd;
  }
  
  public void setFreqCd(String freqCd) {
    this.freqCd = freqCd;
  }
  
  public String getPrevBizDate() {
    return prevBizDate;
  }
  
  public void setPrevBizDate(String prevBizDate) {
    this.prevBizDate = prevBizDate;
  }
  
  public String getCurrBizDate() {
    return currBizDate;
  }
  
  public void setCurrBizDate(String currBizDate) {
    this.currBizDate = currBizDate;
  }
  
  public String getNextBizDate() {
    return nextBizDate;
  }
  
  public void setNextBizDate(String nextBizDate) {
    this.nextBizDate = nextBizDate;
  }
  
  public String getProcFlag() {
    return procFlag;
  }
  
  public void setProcFlag(String procFlag) {
    this.procFlag = procFlag;
  }
  
  private String srcSysCd;
  private String ctryCd;
  private String freqCd;
  private String prevBizDate;
  private String currBizDate;
  private String nextBizDate;
  private String procFlag;
  
  @Override
  public String toString() {
    return new ToStringBuilder(this).append("srcSysCd", srcSysCd)
    		                            .append("ctryCd", ctryCd)
    		                            .append("freqCd", freqCd)
    		                            .append("prevBizDate", prevBizDate)
    		                            .append("currBizDate", currBizDate)
    		                            .append("nextBizDate", nextBizDate)
    		                            .append("procFlag", procFlag)
    		                            .toString();
  }
}
