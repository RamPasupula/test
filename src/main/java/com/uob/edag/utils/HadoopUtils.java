package com.uob.edag.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGValidationException;

public class HadoopUtils {

	private static Logger logger = Logger.getLogger(HadoopUtils.class);

	private static ThreadLocal<FileSystem> fileSystem = new ThreadLocal<FileSystem>() {

		protected FileSystem initialValue() {
			Configuration config = new Configuration();
			config.addResource(new Path(PropertyLoader.getProperty(UobConstants.HADOOP_CONF_DIR) + "core-site.xml"));
			config.addResource(new Path(PropertyLoader.getProperty(UobConstants.HADOOP_CONF_DIR) + "hdfs-site.xml"));
			System.setProperty("hadoop.home.dir", PropertyLoader.getProperty(UobConstants.HADOOP_HOME));
			logger.debug("HADOOP_HOME system property set to " + System.getProperty("hadoop.home.dir"));

			try {

				if (UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.KERBEROS_ENABLED))) {
					logger.debug("Setting Security authentication to kerberos");
					config.set("hadoop.security.authentication", "kerberos");
					UserGroupInformation.setConfiguration(config);
				}
				logger.debug("Accessing HDFS using config: " + config);
				return FileSystem.get(config);
			} catch (EDAGValidationException | IOException e) {
				throw new RuntimeException(e); // throw runtime exception since ThreadLocal.initialValue() doesn't throw
												// checked exception
			}
		}
	};

	public static String getConfigValue(String key) {
		return fileSystem.get().getConf().get(key);
	}

	public static void copyToHDFS(File fileToCopy, String destination) throws EDAGIOException {
		copyToHDFS(fileToCopy, new Path(destination));
	}

	public static void copyToHDFS(File fileToCopy, Path destination) throws EDAGIOException {
		copyToHDFS(false, true, fileToCopy, destination);
	}

	public static boolean checkIfFileExists(String fileNamePath) throws EDAGIOException {
		return checkIfFileExists(new Path(fileNamePath));
	}

	public static boolean checkIfFileExists(Path fileNamePath) throws EDAGIOException {
		try {
			logger.debug("Checking if the path " + fileNamePath + " exists");
			return fileSystem.get().exists(fileNamePath);
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_CHECK_IF_FILE_EXISTS, fileNamePath);
		}
	}

	public static boolean createHDFSPath(String fileNamePath) throws EDAGIOException {
		return createHDFSPath(new Path(fileNamePath));
	}

	public static boolean createHDFSPath(Path fileNamePath) throws EDAGIOException {
		try {
			logger.debug("Checking if the path " + fileNamePath + " exists");
			return fileSystem.get().mkdirs(fileNamePath);
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_CREATE_DIRECTORY, fileNamePath);
		}
	}

	public static List<Path> listFiles(String folder) throws EDAGIOException {
		return listFiles(new Path(folder));
	}

	public static List<Path> listFiles(Path folder) throws EDAGIOException {
		return listFiles(folder, false, true);
	}

	public static List<Path> listFiles(String folder, boolean recursive, boolean throwExceptionIfPathInaccessible)
			throws EDAGIOException {
		return listFiles(new Path(folder), recursive, throwExceptionIfPathInaccessible);
	}

	public static List<Path> listFiles(Path folder, boolean recursive, boolean throwExceptionIfPathInaccessible)
			throws EDAGIOException {
		List<Path> result = new ArrayList<Path>();

		try {
			RemoteIterator<LocatedFileStatus> statuses = fileSystem.get().listFiles(folder, recursive);
			while (statuses.hasNext()) {
				LocatedFileStatus status = statuses.next();
				if (status.isFile()) {
					result.add(status.getPath());
				}
			}
		} catch (IOException e) {
			if (throwExceptionIfPathInaccessible) {
				throw new EDAGIOException(EDAGIOException.CANNOT_LIST_FILES_IN_DIR, folder, e.getMessage());
			} else {
				logger.debug("Unable to list files in " + folder + ": " + e.getMessage());
			}
		}

		logger.info("Files in " + folder + ": " + result);
		return result;
	}

	public static void copyToHDFS(boolean deleteSource, boolean overwrite, File fileToCopy, Path destination) throws EDAGIOException {
		try {
			fileSystem.get().copyFromLocalFile(deleteSource, overwrite, new Path(fileToCopy.getPath()), destination);
			logger.debug(fileToCopy.getPath() + " copied to " + destination.toString() + " in HDFS, delete source = " + deleteSource
					+ ", overwrite = " + overwrite);
		} catch (IllegalArgumentException | IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_COPY_FILE, fileToCopy.getPath(), destination, e.getMessage());
		}
	}

	public static void deleteHDFSFiles(Path destination, boolean recursive) throws EDAGIOException {
		try {
			fileSystem.get().delete(destination, recursive);
		} catch (IllegalArgumentException | IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_DELETE_FILE, destination.getName(), e.getMessage());
		}
	}

	public static InputStream openHDFSFile(String path) throws EDAGIOException {
		return openHDFSFile(new Path(path));
	}

	public static InputStream openHDFSFile(Path path) throws EDAGIOException {
		try {
			return fileSystem.get().open(path);
		} catch (IllegalArgumentException | IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, path, e.getMessage());
		}
	}


	public static long getSizeOfHDFSDirectoryPath(String hdfsPath) throws EDAGIOException {
		return getHDFSDirectoryPath(new Path(hdfsPath));
	}

	public static long getHDFSDirectoryPath(Path hdfsPath) throws EDAGIOException {
		try {
			logger.debug("Retrieving directory size for path " + hdfsPath);
			return fileSystem.get().getContentSummary(hdfsPath).getLength();
		} catch (IOException e) {
			throw new EDAGIOException(EDAGIOException.CANNOT_RETRIEVE_DIRECTORY_SIZE, hdfsPath);
		}
	}

	public static String getHDFSHostName() throws EDAGIOException {
        	return fileSystem.get().getConf().get("dfs.nameservices");
	}

	public static void resetUGI(){
		UserGroupInformation.reset();
		fileSystem.remove();
	}
		
	public static ThreadLocal<FileSystem> getFileSystem() {
		return fileSystem;
	}
}