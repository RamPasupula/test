package com.uob.edag.processor;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.model.LobStampingInfo;
import com.uob.edag.utils.FileUtils;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobInterfaceFileParser;
import com.uob.edag.utils.UobUtils;

public class LobStampingRegistration {

	private static Logger LOG = null;

	private String registrationFilePath;

	private String sqlFileName;

	private boolean forceRegistration;

	public LobStampingRegistration(String registrationFileName, String sqlFileName, boolean forceRegistration) {
		this.registrationFilePath = registrationFileName;
		this.sqlFileName = sqlFileName;
		this.forceRegistration = forceRegistration;
	}

	public String getRegistrationFileName() {
		return registrationFilePath;
	}

	public String getSqlFileName() {
		return sqlFileName;
	}

	public boolean isForceRegistration() {
		return forceRegistration;
	}

	private void generateRegisterScripts() throws EDAGValidationException, EDAGIOException {
		UobInterfaceFileParser uobInterfaceFileParser = new UobInterfaceFileParser();
		List<LobStampingInfo> lobStampingInfos = uobInterfaceFileParser.parseLobRegistrationFile(registrationFilePath);
		generateSQL(lobStampingInfos);
	}

	private void generateSQL(List<LobStampingInfo> lobStampingInfos) throws EDAGIOException {
		List<String> insertSqls = new ArrayList<>();
		List<String> deleteSqls = new ArrayList<>();
		List<String> hiveAlterTableSqls = new ArrayList<>();

		deleteSqls.add("set define off;");
		deleteSqls.add("whenever SQLERROR EXIT ROLLBACK;");

		insertSqls.add("set define off;");
		insertSqls.add("set sqlblanklines on;");
		insertSqls.add("whenever SQLERROR EXIT ROLLBACK;");

		for (LobStampingInfo lobStampingInfo : lobStampingInfos) {
			insertSqls.add(lobStampingInfo.getInsertSQL());
			if (forceRegistration) {
				deleteSqls.add(lobStampingInfo.getDeleteSQL());
			} else {
				hiveAlterTableSqls.add(lobStampingInfo.getHiveAlterTableSQL());
			}
		}

		insertSqls.add("COMMIT;");
		deleteSqls.add("COMMIT;");

		String insertSqlFileName = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, "")
				.replace(UobConstants.FILENM_PARAM, UobConstants.LOB_REGISTRATION_FILE_NAME)
				.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_INSERT);

		String deleteSqlKeyFileName = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, "")
				.replace(UobConstants.FILENM_PARAM, UobConstants.LOB_REGISTRATION_FILE_NAME)
				.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.METADATA_DELETE);

		String hiveCreateSqlFileName = sqlFileName.replace(UobConstants.SRC_SYS_PARAM, "")
				.replace(UobConstants.FILENM_PARAM, UobConstants.LOB_REGISTRATION_FILE_NAME)
				.replace(UobConstants.QUERY_TYPE_PARAM, UobConstants.HIVE_CREATE);

		createSqlFile(insertSqlFileName, insertSqls);
		if (forceRegistration) {
			createSqlFile(deleteSqlKeyFileName, deleteSqls);
		} else {
			createSqlFile(hiveCreateSqlFileName, hiveAlterTableSqls);
		}

	}

	private void createSqlFile(String sqlFileName, List<String> sqlToSave) throws EDAGIOException {
		String sqlFileLocation = PropertyLoader.getProperty(UobConstants.SQL_FILE_LOC);
		String sqlFileArchiveLocation = PropertyLoader.getProperty(UobConstants.SQL_FILE_ARCHIVE_LOCATION);
		String fullFileName = sqlFileLocation + File.separator + sqlFileName;
		File sqlFile = new File(fullFileName);
		if (sqlFile.exists()) { // Archive existing SQL file
			String sqlArchiveFileName = null;
			String basename = FilenameUtils.getBaseName(sqlFileName);
			String extension = FilenameUtils.getExtension(sqlFileName);
			DateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			sqlArchiveFileName = sqlFileArchiveLocation + File.separator + basename + UobConstants.UNDERSCORE
					+ sdf.format(System.currentTimeMillis()) + "." + extension;
			boolean renamed = sqlFile.renameTo(new File(sqlArchiveFileName));
			if (!renamed) {
				throw new EDAGIOException(EDAGIOException.CANNOT_MOVE_FILE, sqlFile.getPath(), sqlArchiveFileName,
						"SQL file cannot be archived");
			}
			LOG.info("Existing SQL file " + sqlFileName + " archived successfully to " + sqlArchiveFileName);
		}

		FileUtils fileUtil = new FileUtils();
		fileUtil.writeToFile(sqlToSave, sqlFile);
		LOG.info("New SQL file created successfully:" + fullFileName);
	}

	public static void main(String[] args) throws Exception {

		CommandLineParser parser = new DefaultParser();
		Options options = new Options();

		Option help = new Option("h", "help", false, "Show Help");
		options.addOption(help);

		Option forceFlag = new Option("f", "force", true, "Forceful Registration of Stamping SQL file");
		forceFlag.setArgs(0);
		forceFlag.setRequired(false);
		forceFlag.setArgName("force");
		options.addOption(forceFlag);

		Option registration = new Option("r", "register", true, "Registration of Stamping SQL file");
		registration.setArgs(1);
		registration.setRequired(true);
		registration.setArgName("register");
		options.addOption(registration);

		try {
			boolean force = false;
			CommandLine command = parser.parse(options, args);
			if (command.hasOption("h")) {
				showHelp(options);
				System.exit(0);
			}

			if (command.hasOption("f")) {
				force = true;
			}

			String registrationFile = command.getOptionValue("r");

			File lobRegistrationFile = new File(registrationFile);
			if (!lobRegistrationFile.exists()) {
				throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, registrationFile, "File doesn't exist or is a directory");
			}
			String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
			String logFileName = "EDA_DL_" + lobRegistrationFile.getName() + "_" + execDate + ".log";
			System.setProperty("logFileName", logFileName);
			LOG = Logger.getLogger(RegistrationGenerator.class);
			UobUtils.logJavaProperties();
			UobUtils.logPackageProperties();

			String sqlFileName = PropertyLoader.getProperty(UobConstants.SQL_FILE_NM);

			LobStampingRegistration lobStampingRegistration = new LobStampingRegistration(registrationFile, sqlFileName, force);
			lobStampingRegistration.generateRegisterScripts();

		} catch (ParseException e) {
			showHelp(options);
			LOG.error("Exception while parsing the command line arguments");
			LOG.error(ExceptionUtils.getStackTrace(e));
			System.exit(1);
		} catch (Exception e) {
			LOG.error("Exception while registering LOB Stamping tables");
			LOG.error(ExceptionUtils.getStackTrace(e));
		}

	}

	private static void showHelp(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("LOB Stamping Registration", options);

	}

}