package com.uob.edag.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.JarFile;

import org.apache.log4j.Logger;

import com.uob.edag.exception.EDAGIOException;

public class ZipUtils {
	
	protected Logger logger = Logger.getLogger(getClass());
	
	private static ThreadLocal<ZipUtils> utils = new ThreadLocal<ZipUtils>() {
		
		public ZipUtils initialValue() {
			return new ZipUtils();
		}
	};
	
	public class Manifest extends java.util.jar.Manifest {
		
		public Manifest(java.util.jar.Manifest parent) {
			super(parent);
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			
			Attributes attr = this.getMainAttributes();
			for (Object attrKey : attr.keySet()) {
				sb.append(attrKey + ": " + attr.get(attrKey) + "\n");
			}
			
			return sb.toString();
		}
	}

	public static void main(String[] args) {
		try {
			if ("-showmanifest".equalsIgnoreCase(args[0])) {
				File jarFile = args.length > 1 ? new File(args[1]) : getCurrentJarFile();
				if (jarFile == null) {
					System.out.println("This application is not contained in a valid jar file");
					System.exit(-1);
				} else if (!jarFile.isFile()) {
					System.out.println(args[1] + " is not file");
					System.exit(-1);
				}
				
				Manifest mf = getManifest(jarFile);
				if (mf != null) {
					System.out.println(mf);
				} else {
					System.out.println(jarFile.getPath() + " doesn't contain manifest file");
					System.exit(-1);
				}
			}
		} catch (Exception e) {
			System.out.println("Unsupported operation: " + e.getMessage());
		  System.out.println("Supported argument: -showmanifest [jarFile]");
		}
	}
	
	public static File getCurrentJarFile() {
		return utils.get().internalGetCurrentJarFile();
	}

	private File internalGetCurrentJarFile() {
		URL url = getClass().getResource("/" + getClass().getName().replace('.', '/') + ".class");
		logger.debug("URL of file containing class " + getClass().getName() + " is " + url);
		String urlString = url.getPath();
		int startPos = urlString.startsWith("file:") ? 5 : 0;
		int endPos = urlString.indexOf("!");
		urlString = endPos > -1 ? urlString.substring(startPos, endPos) : urlString.substring(startPos);
		
		try (JarFile jarFile = new JarFile(urlString)) {
			logger.debug("Current application is contained in " + urlString);
		} catch (IOException e) {
			logger.warn(urlString + " is not a valid jar file");
			return null;
		}
		
		return new File(urlString);
	}

	public static Manifest getManifest(File file) throws EDAGIOException {
		return utils.get().internalGetManifest(file);
	}

	private Manifest internalGetManifest(File file) throws EDAGIOException {
		Manifest mf = null;
		boolean closingJar = false;
		try (JarFile jarFile = new JarFile(file)) {
			mf = new Manifest(jarFile.getManifest());
			closingJar = true;
		} catch (IOException e) {
			if (closingJar) {
				logger.warn("Unable to close jar file " + file.getPath() + ": " + e.getMessage());
			} else {
				throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, file.getPath(), e.getMessage());
			}
		}
		
		return mf;
	}
}
