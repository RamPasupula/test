package com.uob.edag.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @Author : Daya Venkatesan
 * @Date of Creation: 11/08/2016
 * @Description : The file defines the standardization rules configured on the
 *              framework.
 * 
 */

public class RuleModel {

  private int ruleId; // Uniquely generated Rule ID
  private String ruleDesc; // Description of the Rule applied on Informatica

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("ruleId", ruleId)
    		                            .append("ruleDesc", ruleDesc)
    		                            .toString();
  }

  public int getRuleId() {
    return ruleId;
  }

  public void setRuleId(int ruleId) {
    this.ruleId = ruleId;
  }

  public String getRuleDesc() {
    return ruleDesc;
  }

  public void setRuleDesc(String ruleDesc) {
    this.ruleDesc = ruleDesc;
  }
}
