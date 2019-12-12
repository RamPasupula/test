package com.uob.edag.log4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Layout;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

public class MultiPatternLayout extends Layout {
	
	public static final String DEFAULT = "default";
	
	private List<PatternLayout> patternLayouts = new ArrayList<PatternLayout>(); 
	private Map<String, Integer> patternIndexes = new HashMap<String, Integer>();
	private Map<String, PatternLayout> patternMap = new HashMap<String, PatternLayout>();
	private PatternLayout defaultLayout = null;
	private boolean resetPatternMap = false;

	@Override
	public void activateOptions() {
		// do nothing
	}

	@Override
	public String format(LoggingEvent event) {
		if (resetPatternMap) {
			patternMap.clear();
			for (String loggerName : patternIndexes.keySet()) {
				Integer patternIndex = patternIndexes.get(loggerName);
				PatternLayout layout = patternLayouts.get(patternIndex);
				patternMap.put(loggerName, layout);
				
				if (DEFAULT.equals(loggerName)) {
					defaultLayout = layout;
				}
			}
			
			resetPatternMap = false;
		}
		
		PatternLayout layout = patternMap.get(event.getLoggerName());
		return layout == null ? defaultLayout.format(event) : layout.format(event);
	}
	
	public void setConversionPattern(String pattern) {
		patternLayouts.add(new PatternLayout(pattern));
	}
	
	public void setPatternSelector(String selector) {
		patternMap.clear();
		
		for (String pair : selector.split(",", -1)) {
			String[] keyValuePair = pair.split("=", -1);
			String key = keyValuePair[0];
			Integer value = Integer.parseInt(keyValuePair[1]);
			patternIndexes.put(key, value);
		}
		
		resetPatternMap = true;
	}

	@Override
	public boolean ignoresThrowable() {
		return true;
	}
}
