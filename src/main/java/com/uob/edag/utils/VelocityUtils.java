package com.uob.edag.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.context.Context;
import org.apache.velocity.runtime.RuntimeConstants;

import com.uob.edag.exception.EDAGIOException;

public class VelocityUtils {
	
	private static final Logger logger = Logger.getLogger(VelocityUtils.class);
	
	private static ThreadLocal<VelocityEngine> ve = new ThreadLocal<VelocityEngine>() {
		
		protected VelocityEngine initialValue() {
			VelocityEngine engine = new VelocityEngine();
			
			engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute" );
      engine.setProperty("runtime.log.logsystem.log4j.logger", logger.getName());
			engine.init();
			logger.debug("Velocity engine initialized for thread " + Thread.currentThread().getName());
			
			return engine;
		}
	};
	
	public static boolean evaluate(Context ctx, Writer writer, String logTag, String template) {
		return ve.get().evaluate(ctx, writer, logTag, template);
	}
	
	public static boolean evaluate(Context ctx, Writer writer, String logTag, Reader reader) {
		return ve.get().evaluate(ctx, writer, logTag, reader);
	}
	
	public static boolean evaluate(Context ctx, File outputFile, String logTag, Reader reader, Charset charset) throws EDAGIOException {
		boolean result = false;
		Charset chr = charset == null ? Charset.defaultCharset() : charset;
		
		boolean closingWriter = false;
		try (Writer out = new OutputStreamWriter(new FileOutputStream(outputFile), chr)) {
			result = evaluate(ctx, out, logTag, reader);
			logger.debug("Template evaluated into " + outputFile.getPath());
			
			closingWriter = true;
		} catch (IOException e) {
			if (closingWriter) {
				logger.warn("Unable to close " + outputFile.getPath() + ": " + e.getMessage());	
			} else {
				throw new EDAGIOException(EDAGIOException.CANNOT_WRITE_FILE, outputFile.getPath(), e.getMessage());
			}
		}	
		
		return result;
	}
	
	public static boolean evaluate(Context ctx, File outputFile, String logTag, File templateFile) throws EDAGIOException {
		return evaluate(ctx, outputFile, logTag, templateFile, null);
	}
	
	public static boolean evaluate(Context ctx, File outputFile, String logTag, File templateFile, Charset charset) throws EDAGIOException {
		boolean result = false;
		Charset chr = charset == null ? Charset.defaultCharset() : charset;
		
		boolean closingReader = false;
		try (Reader reader = new InputStreamReader(new FileInputStream(templateFile), chr)) {
			result = evaluate(ctx, outputFile, logTag, reader, chr);
			logger.debug(templateFile.getPath() + " evaluated into " + outputFile.getPath());
			
			closingReader = true;
		} catch (IOException e) {
			if (closingReader) {
				logger.warn("Unable to close " + templateFile + ": " + e.getMessage());
			} else {
				throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, templateFile.getPath(), e.getMessage());
			}
		}
		
		return result;
	}
	
	public static String getTemplateFromFile(File templateFile, boolean flattenTemplate) throws EDAGIOException {
		String template = null;
		boolean closingStream = false;
		try (InputStream fis = new FileInputStream(templateFile)) {
			template = getTemplateFromInputStream(fis, flattenTemplate);
			
			closingStream = true;
		} catch (IOException e) {
			if (closingStream) {
				logger.warn("Unable to close stream from " + templateFile.getPath() + ": " + e.getMessage());
			} else {
				throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, templateFile.getPath(), e.getMessage());
			}
		}
		
		return template;
	}
	
	public static String getTemplateFromFile(File file) throws EDAGIOException {
		return getTemplateFromFile(file, false);
	}
	
	public static String getTemplateFromResource(String resourceName) throws EDAGIOException {
		return getTemplateFromResource(resourceName, false);
	}
	
	public static String getTemplateFromInputStream(InputStream stream) throws EDAGIOException {
		return getTemplateFromInputStream(stream, false);
	}
	
	public static String getTemplateFromInputStream(InputStream stream, boolean flattenTemplate) throws EDAGIOException {
		StringBuilder template = new StringBuilder();
  	
  	boolean closingReader = false;
  	try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
  		String line = null;
  		while ((line = reader.readLine()) != null) {
  			template.append(flattenTemplate ? org.apache.commons.lang3.StringUtils.stripStart(line, null) : line);
  		}
  		
  		closingReader = true;
  	} catch (IOException e) {
			if (closingReader) {
				logger.warn("Unable to close input stream reader: " + e.getMessage());
			} else {
				throw new EDAGIOException(EDAGIOException.CANNOT_READ_INPUT_STREAM, e.getMessage());
			}
		}
  	
  	String templateContent = template.toString();
  	logger.debug("Template read from input stream: " + templateContent);
  	return templateContent;
	}
	
	public static String getTemplateFromResource(String resourceName, boolean flattenTemplate) throws EDAGIOException {
		return getTemplateFromInputStream(VelocityUtils.class.getResourceAsStream(resourceName), flattenTemplate);
	}
}
