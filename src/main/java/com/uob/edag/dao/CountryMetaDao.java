/**
 * 
 */
package com.uob.edag.dao;

import java.util.HashMap;
import java.util.List;

import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;

import com.uob.edag.connection.DBMSConnectionFactory;
import com.uob.edag.connection.MyBatisConnectionFactory;
import com.uob.edag.exception.EDAGException.PredefinedException;
import com.uob.edag.exception.EDAGMyBatisException;
import com.uob.edag.mappers.CountryMetaMapper;

/**
 * @author kg186041
 *
 */
public class CountryMetaDao extends AbstractDao {
	
	
    public static final PredefinedException CANNOT_GET_COUNTRY_META = new PredefinedException(CountryMetaDao.class, "CANNOT_GET_COUNTRY_META", "Unable to get result from Country Meta : {0}");
	   
	
    public CountryMetaDao() {
    	super(DBMSConnectionFactory.getFactory());
	 
	} 
		
	
	 public List<HashMap<String,String>> getCountryMetaList(HashMap<String,String> param) throws EDAGMyBatisException {
	    try (SqlSession session = openSession()) { 
	    	List<HashMap<String,String>>   countryList = session.getMapper(CountryMetaMapper.class).getCountryMetaList(param);
	      logger.debug((countryList == null ? 0 : countryList.size()) + " country meta ");
	      return countryList;
	    } catch (PersistenceException e) {
	    	throw new EDAGMyBatisException(CountryMetaDao.CANNOT_GET_COUNTRY_META, e.getMessage());
	    }
	  }
	
}	
