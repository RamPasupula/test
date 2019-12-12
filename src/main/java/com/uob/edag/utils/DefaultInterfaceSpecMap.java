package com.uob.edag.utils;

import java.util.HashMap;
import java.util.Map;

import com.uob.edag.exception.EDAGValidationException;

public class DefaultInterfaceSpecMap extends HashMap<String, Object> implements InterfaceSpecMap {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1525181174544318506L;

	public DefaultInterfaceSpecMap() {
		super();
	}

	public DefaultInterfaceSpecMap(int initialCapacity) {
		super(initialCapacity);
	}

	public DefaultInterfaceSpecMap(Map<String, ? extends Object> m) {
		super(m);
	}

	public DefaultInterfaceSpecMap(int initialCapacity, float loadFactor) {
		super(initialCapacity, loadFactor);
	}

	@Override
	public <P> P get(String key, Class<P> valueType) throws EDAGValidationException {
		Object value = super.get(key);
		
		if (value == null) {
			return null;
		} else if (valueType.isAssignableFrom(value.getClass())) {
			return valueType.cast(value);
		} else {
			throw new EDAGValidationException(EDAGValidationException.INVALID_TYPE, key, valueType.getName(), value.getClass().getName());
		}
	}

	@Override
	public <P> P put(String key, Object value, Class<P> oldValueType) throws EDAGValidationException {
		Object oldValue = null;
		String trimmedKey = key == null ? null : key.trim();
		
		if (value instanceof String) {
			oldValue = super.put(trimmedKey, value.toString().trim());
		} else {
			oldValue = super.put(trimmedKey, value);
		}
		
		if (oldValue == null) {
			return null;
		} else if (oldValueType.isAssignableFrom(oldValue.getClass())) {
			return oldValueType.cast(oldValue);
		} else {
			throw new EDAGValidationException(EDAGValidationException.INVALID_TYPE, key, oldValueType.getName(), oldValue.getClass().getName());
		}
	}

	@Override
	public <P> P remove(String key, Class<P> removedValueType) throws EDAGValidationException {
		Object removedValue = super.remove(key);
		
		if (removedValue == null) {
			return null;
		} else if (removedValueType.isAssignableFrom(removedValue.getClass())) {
			return removedValueType.cast(removedValue);
		} else {
			throw new EDAGValidationException(EDAGValidationException.INVALID_TYPE, key, removedValueType.getName(), removedValue.getClass().getName());
		}
	}
}
