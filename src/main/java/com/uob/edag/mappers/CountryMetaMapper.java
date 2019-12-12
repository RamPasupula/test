package com.uob.edag.mappers;

import java.util.HashMap;
import java.util.List;

//import com.uob.edag.model.RuleModel;
//import com.uob.edag.model.SolrDocCollectModel;
//import com.uob.edag.model.SolrReferenceModel;

/**
 * @author kg186041
 *
 */
public interface CountryMetaMapper {
	 
 
	public List<HashMap<String,String>> getCountryMetaList(HashMap<String,String> param);
	  
}
