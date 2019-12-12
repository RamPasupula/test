package com.uob.edag.connection;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGSSHException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.PropertyLoader;

public class SshConnectionFactory {
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private static SshConnectionFactory factory = null;
	
	public static SshConnectionFactory getFactory() {
		if (factory == null) {
			factory = new SshConnectionFactory();
		}
		
		return factory;
	}
  
  /**
   * This method is used to get a Jsch Session Object which is a remote connection
   * @return the Session object
   * @throws UobException when the deployment node is missing in configuration
   * @throws Exception when there is error creating the connection.
   */
  public Session getJschSession(String node) throws EDAGValidationException, EDAGSSHException {
    String userName = null;
    String hostName = null;
    String key = null;
    
    if (StringUtils.isBlank(node)) {
      throw new EDAGValidationException(EDAGValidationException.EMPTY_VALUE, "Deployment Node", "Deployment Node cannot be empty");
    }
    
    if (UobConstants.NODE1.equalsIgnoreCase(node.trim())) {
      hostName = PropertyLoader.getProperty(UobConstants.NODE1);
      userName = PropertyLoader.getProperty(UobConstants.NODE1_USER);
      key = PropertyLoader.getProperty(UobConstants.NODE1_PWD);
    } else if (UobConstants.NODE2.equalsIgnoreCase(node.trim())) {
      hostName = PropertyLoader.getProperty(UobConstants.NODE2);
      userName = PropertyLoader.getProperty(UobConstants.NODE2_USER);
      key = PropertyLoader.getProperty(UobConstants.NODE2_PWD);
    } else if (UobConstants.NODE3.equalsIgnoreCase(node.trim())) {
      hostName = PropertyLoader.getProperty(UobConstants.NODE3);
      userName = PropertyLoader.getProperty(UobConstants.NODE3_USER);
      key = PropertyLoader.getProperty(UobConstants.NODE3_PWD);
    } 

    Session session = null;
    try {
	    JSch jsch = new JSch();
	    jsch.addIdentity(key);
	    logger.debug("Password added into Jsch instance");
	    
	    // TODO don't hardcode the port
	    session = jsch.getSession(userName, hostName, 22);
	    logger.info("Established SSH session to " + hostName + " as " + userName + " on port 22");
    } catch (JSchException e) {
    	throw new EDAGSSHException(EDAGSSHException.CANNOT_GET_SESSION, hostName, 22, userName, e.getMessage());
    }
    
    return session;
  }
}
