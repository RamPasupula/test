package com.uob.edag.connection;

import java.util.Properties;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public abstract class KerberizedMyBatisConnectionFactory extends MyBatisConnectionFactory {

    @Override
    protected void init(String resourceName, Properties connectionProperties) throws EDAGIOException, EDAGMyBatisException {
        boolean kerberosEnabled = false;
        try {
            kerberosEnabled = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.KERBEROS_ENABLED));
        } catch (EDAGValidationException e) {
            throw new EDAGIOException(e.getMessage());
        }

        // Set security context
        if (kerberosEnabled) {
            System.setProperty("hadoop.home.dir", PropertyLoader.getProperty(UobConstants.HADOOP_HOME));
            logger.debug("HADOOP_HOME system property set to " + System.getProperty("hadoop.home.dir"));
            Configuration config = new Configuration();
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);
            logger.debug("Hadoop security authentication set to " + config.get("hadoop.security.authentication"));
        }

        super.init(resourceName, connectionProperties);
    }
}
