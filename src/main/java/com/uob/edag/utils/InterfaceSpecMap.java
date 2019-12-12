package com.uob.edag.utils;

import java.util.Map;

import com.uob.edag.exception.EDAGValidationException;

public interface InterfaceSpecMap extends Map<String, Object> {

	<P> P get(String key, Class<P> dataType) throws EDAGValidationException;
	
	<P> P put(String key, Object value, Class<P> oldValueType) throws EDAGValidationException;
	
	<P> P remove(String key, Class<P> oldValueType) throws EDAGValidationException;
}
