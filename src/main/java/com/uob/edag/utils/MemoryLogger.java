package com.uob.edag.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

public class MemoryLogger implements Runnable {
	
	public static final int DEFAULT_DELAY = 10;
	public static final String MEMORY_CHECK_CMD_PROPERTY = MemoryLogger.class.getName() + ".MemoryCheckCmd";
	public static final String DELAY_PROPERTY = MemoryLogger.class.getName() + ".Delay";
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private int delay = DEFAULT_DELAY; // delay in seconds
	private String cmd;
	
	public MemoryLogger() {
		String delay = PropertyLoader.getProperty(DELAY_PROPERTY);
		if (delay != null) {
			this.delay = Integer.parseInt(delay);
		}
		cmd = org.apache.commons.lang3.StringUtils.trimToNull(PropertyLoader.getProperty(MEMORY_CHECK_CMD_PROPERTY));
	}

	@Override
	public void run() {
		if (cmd != null) {
			Timer timer = new Timer(true);
	    TimerTask timerTask = new TimerTask() {
	
				@Override
				public void run() {
					Runtime r = Runtime.getRuntime();
					try {
						Process p = r.exec(cmd);
						p.waitFor();
						String line = null;
						try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
							while ((line = reader.readLine()) != null) {
								logger.info(line);
							}
						}
						line = null;
						try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
							while ((line = reader.readLine()) != null) {
								logger.info(line);
							}
						}
					} catch (IOException | InterruptedException e) {
						logger.warn("Unable to execute " + cmd + ": " + e.getMessage());
					}
				}
	    };
	    
	  	timer.schedule(timerTask, 0, delay * 1000);
		}
	}
}
