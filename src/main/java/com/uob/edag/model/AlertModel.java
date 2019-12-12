package com.uob.edag.model;

import java.sql.Time;
import java.text.MessageFormat;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.RegistrationDao;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.utils.UobUtils;

/**
 * @Author       : Daya Venkatesan
 * @Date of Creation: 10/24/2016
 * @Description    : The file defines the alerts configured on every process.
 * 
 */

public class AlertModel {
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private RegistrationDao regDao = new RegistrationDao();
	
	private static final MessageFormat INSERT_ALERT_TEMPLATE = new MessageFormat(
			UobUtils.ltrim("INSERT INTO EDAG_ALERTS(ALERT_ID, PROC_ID, ALERT_EMAIL, CRT_DT, CRT_USR_NM) ") +
      UobUtils.ltrim("VALUES({0, number, #}, ''{1}'', ''{2}'', SYSDATE, ''{3}''); ")
	);
	
	public String getInsertAlertSql() throws EDAGMyBatisException {
		String result = INSERT_ALERT_TEMPLATE.format(new Object[] {regDao.selectAlertId(), 
																															 getProcId(), getEmail(),
			  																										   UobConstants.SYS_USER_NAME});
		logger.debug("Insert alert statement: " + result);
		return result;
	}
  
  public String getProcId() {
    return procId;
  }
  
  public void setProcId(String procId) {
    this.procId = procId == null ? null : procId.trim();
  }
  
  public int getAlertId() {
    return alertId;
  }
  
  public void setAlertId(int alertId) {
    this.alertId = alertId;
  }
  
  public Time getSla() {
    return sla;
  }
  
  public void setSla(Time sla) {
    this.sla = sla;
  }
  
  public String getEmail() {
    return email;
  }
  
  public void setEmail(String email) {
    this.email = email == null ? null : email.trim();
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this).append("procID", procId)
    		                            .append("alertID", alertId)
    		                            .append("sla", sla)
    		                            .append("email", email)
    		                            .toString();
  }
  
  private String procId;    // Process ID for which the alert is configured
  private int alertId;      // Uniquely generated Alert ID
  private Time sla;         // SLA Time after which the alert will be sent
  private String email;     // Email ID's to which the alert will be sent
}
