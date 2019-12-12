package com.uob.edag.processor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.uob.edag.constants.UobConstants;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.exception.EDAGProcessorException;
import com.uob.edag.exception.EDAGSQLException;
import com.uob.edag.exception.EDAGSecurityException;
import com.uob.edag.exception.EDAGValidationException;
import com.uob.edag.utils.PropertyLoader;
import com.uob.edag.utils.UobUtils;

/**
 * Created by cb186046 on 8/3/17.
 * Program that reads a file containing a list of HQL files given as parameter.
 * If HQL file names are prefixed with character '@' then they must be processed.
 * Utilisation example:
 *
 * java \
 * -Dlog4j.configuration=file:///Users/cb186046/IdeaProjects/edf/log4j-cb.properties \
 * -Dframework-conf.properties=/Users/cb186046/IdeaProjects/edf/framework-conf-cb.properties \
 * -Ddatabase.properties=/Users/cb186046/IdeaProjects/edf/database-cb.properties \
 * -cp edf.jar:Users/cb186046/IdeaProjects/edf/lib/* \
 * com.uob.edag.processor.DataLakeDeployment -d /Users/cb186046/IdeaProjects/edf/sql/hql.list
 */
public class DataLakeDeployment {

    private static Logger logger = null;
    private static Options options = new Options();
    private static Connection con = null;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public static void main(String[] arguments) throws EDAGProcessorException, EDAGIOException, EDAGSQLException, EDAGSecurityException, EDAGValidationException {
        // List of options
        // Help option
        options.addOption("h", "help", false, "Show Help");
        // Deployment option
        Option deployment = new Option("d", "deploy", true, "Run DL deployment Process for given Parameters");
        deployment.setArgs(1);
        deployment.setArgName("Path to file containing the list of HQL files to be deployed");
        options.addOption(deployment);
        // Reset option
        options.addOption("r", "reset", false, "Reset list and HQL files back to initial registration state");

        // Read and parse arguments
        CommandLineParser parser = new DefaultParser();
        CommandLine command;
        try {
            command = parser.parse(options, arguments);
        } catch (ParseException e) {
            throw new EDAGProcessorException(EDAGProcessorException.CANNOT_PARSE_CLI_OPTIONS, e.getMessage());
        }

        if (command.hasOption("h")) {
            DataLakeDeployment.showHelp();
            System.exit(0);
        }

        if (!command.hasOption("d")) {
            throw new EDAGProcessorException(EDAGProcessorException.MISSING_MANDATORY_OPTION, "-d", "Deployment option is mandatory. Use -h for help.");
        }

        String[] args = command.getOptionValues("d");
        if (args == null || args.length < 1) {
            throw new EDAGProcessorException(EDAGProcessorException.INCORRECT_ARGS_FOR_OPTION, "-d", "Not enough arguments passed to run DL deployment.");
        }

        String hqlListPath = args[0];
        // Check if file exist
        File hqlListFile = new File(hqlListPath);
        if (!hqlListFile.isFile()) {
            throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, hqlListFile, "File does not exit or is a directory.");
        }

        // Logs setting
        String execDate = new SimpleDateFormat("yyyyMMddHHmmss").format(System.currentTimeMillis());
        String logFileName = "DL_deployment_" + hqlListFile.getName() + "_" + execDate + ".log";
        System.setProperty("logFileName", logFileName);
        logger = Logger.getLogger(DataLakeDeployment.class);
        UobUtils.logJavaProperties();
        UobUtils.logPackageProperties();
        logger.info("Deployment starts: " + execDate);
        logger.info("Opening and reading: " + hqlListPath);

        if (command.hasOption("r")) {
            DataLakeDeployment.reset(hqlListFile);
        } else {
            DataLakeDeployment.parseFile(hqlListFile);
        }
    }

    private static void parseFile(File hqlListFile) throws EDAGIOException, EDAGSQLException, EDAGSecurityException, EDAGValidationException {
        String env = PropertyLoader.getProperty(UobConstants.ENVIRONMENT) + PropertyLoader.getProperty(UobConstants.ENVIRONMENT_NUM);
        List<String> hqlFilesList;
        ArrayList<String> successfulFiles = new ArrayList<>();
        ArrayList<String> successStatements = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(hqlListFile.toString()))) {
            hqlFilesList = stream
                .filter(line -> line.startsWith("@"))
                .collect(Collectors.toList());
        } catch (IOException e2) {
            throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, hqlListFile, e2.getMessage());
        }
        createHiveConnection();
        for (String hqlFileName : hqlFilesList) {
            List<String> sqlStatementsList;
            if (hqlFileName.trim().substring(0, 1).equals("@")) {
                String fileToBeProcessed = hqlFileName.replace("@", "");
                Path hFilePath = Paths.get(PropertyLoader.getProperty(UobConstants.SQL_FILE_LOC), fileToBeProcessed);
                logger.debug("Will read: " + hFilePath);
                try (Stream<String> stream = Files.lines(hFilePath)) {
                    sqlStatementsList = stream
                        .filter(line -> (line.length()) > 0 && !line.startsWith("--") )
                        .collect(Collectors.toList());
                    for (String sqlStmt : sqlStatementsList) {
                        String origStmt = sqlStmt;
                        sqlStmt = sqlStmt.replace(UobConstants.ENVIRONMENT_STR_PARAM, env);
                        sqlStmt = sqlStmt.replace(";", "");
                        try {
                            Statement stmt = con.createStatement();
                            stmt.execute(sqlStmt);
                            logger.debug(sqlStmt + " executed successfully");
                            stmt.close();
                        } catch (SQLException e) { // error
                            String err = "Unable to execute " + sqlStmt + " from file " + hqlFileName + ": " + e.getMessage();
                            System.out.println("An error has occurred causing the deployment to fail. Please check the logs file.");
                            if (sqlStmt.matches("^ALTER TABLE .+ RENAME TO .+$")
                                && (e.getErrorCode() == 10001)) { // If table not found exception on ALTER RENAME then keep going...
                                logger.warn(err);
                                continue;
                            } else {
                                DataLakeDeployment.closeHiveConnection();
                                if (!successStatements.isEmpty()) {
                                    DataLakeDeployment.replaceInFiles(hFilePath, successStatements, "--", true);
                                }
                                logger.error(err);
                                System.exit(1);
                            }
                        }
                        successStatements.add(origStmt);
                    }
                    // All statements within the hql file have been successful therefore
                    // we can clean the file, remove the @ sign for this file from the list
                    successfulFiles.add(fileToBeProcessed);
                    DataLakeDeployment.replaceAllInFile(hFilePath, "--", "");
                    DataLakeDeployment.replaceAllInFile(hqlListFile.toPath(), hqlFileName, fileToBeProcessed);
                    
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                    File targetFile = new File(PropertyLoader.getProperty(UobConstants.SQL_FILE_ARCHIVE_LOCATION) +
                                               fileToBeProcessed + "." + sdf.format(timestamp));
                    FileUtils.copyFile(hFilePath.toFile(), targetFile);
                    logger.debug(hFilePath + " copied to " + targetFile.getPath());
                    
                    // Uncomment precedent runs
                    logger.info("Execution of commands in " + hFilePath + " completed successfully.");
                } catch (IOException e1) {
                    throw new EDAGIOException(EDAGIOException.CANNOT_READ_FILE, hFilePath, e1.getMessage());
                }
            } else {
            	successfulFiles.add(hqlFileName);
            }
        }
        
        System.out.println("Deployment completed successfully.");
        closeHiveConnection();
        
        if (successfulFiles.size() == hqlFilesList.size()) {
        	reset(hqlListFile);
        }
    }

    private static void replaceAllInFile(Path fileToModify, String pattern, String replace) {
        try {
            Charset charset = StandardCharsets.UTF_8;
            String content = new String(Files.readAllBytes(fileToModify), charset);
            content = content.replaceAll(pattern, replace);
            Files.write(fileToModify, content.getBytes(charset));
        } catch (IOException e) {
            logger.warn("Unable to replace all " + pattern + " in " + fileToModify.toFile().getPath() +
                        " with " + replace + ": " + e.getMessage());
        }
    }

    private static void replaceInFiles(Path fileToModify, List<String> patterns, String prefix,
                                       Boolean add) {
        Charset charset = StandardCharsets.UTF_8;
        String content;
        try {
            content = new String(Files.readAllBytes(fileToModify), charset);
            for (String pattern : patterns) {
                if (add) {
                    if (pattern.indexOf(prefix) < 0) {
                        content = content.replace(pattern, prefix + pattern);
                    }
                } else {
                    content = content.replace(prefix + pattern, pattern);
                }
            }

            Files.write(fileToModify, content.getBytes(charset));
        } catch (IOException e) {
            logger.warn("Unable to modify " + fileToModify.toFile().getPath() + " using prefix " + prefix +
                        ": " + e.getMessage());
        }
    }

    private static void reset(File hqlListFile) throws EDAGIOException {
        ArrayList<String> successfulFiles = new ArrayList<>();
        try (Scanner listScanner = new Scanner(hqlListFile)) {
            while (listScanner.hasNext()) {
                String hqlFile = listScanner.nextLine();
                String fileToBeProcessed = hqlFile.replace("@", "");
                Path hFilePath = Paths.get(PropertyLoader.getProperty(UobConstants.SQL_FILE_LOC), fileToBeProcessed);
                logger.debug("Will read: " + hFilePath);
                DataLakeDeployment.replaceAllInFile(hFilePath, "--", "");
                successfulFiles.add(hqlFile);
            }

            if (!successfulFiles.isEmpty()) {
                DataLakeDeployment.replaceInFiles(hqlListFile.toPath(), successfulFiles, "@", true);
            }
        } catch (FileNotFoundException e) {
            throw new EDAGIOException(EDAGIOException.FILE_NOT_FOUND, hqlListFile.getPath(), e.getMessage());
        }
        
        logger.debug(hqlListFile.getPath() + " has been reset");
    }

    private static void showHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DataLakeDeployment", options);
    }

    private static void createHiveConnection() throws EDAGSQLException, EDAGSecurityException, EDAGValidationException {
        String driverClassName = PropertyLoader.getProperty("Hive.JDBC.Driver");
        try {
            Class.forName(driverClassName);
            logger.debug("Driver " + driverClassName + " registered");
        } catch (ClassNotFoundException e) {
            throw new EDAGSQLException(EDAGSQLException.NO_JDBC_DRIVER, driverClassName, e.getMessage());
        }

        boolean isKerberosEnabled = UobUtils.parseBoolean(PropertyLoader.getProperty(UobConstants.KERBEROS_ENABLED));
        String url = PropertyLoader.getProperty("Hive.JDBC.ConnectionURL");
        try {
            if (isKerberosEnabled) {
                System.setProperty("hadoop.home.dir", PropertyLoader.getProperty(UobConstants.HADOOP_HOME));
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("hadoop.security.authentication", "Kerberos");
                UserGroupInformation.setConfiguration(conf);
                con = DriverManager.getConnection(url);
            } else {
                con = DriverManager.getConnection(url, PropertyLoader.getProperty("Hive.JDBC.Username"), "");
            }

            logger.debug("Connection to " + url + " established");
        } catch (SQLException e) {
            throw new EDAGSQLException(EDAGSQLException.CANNOT_CONNECT_TO_DB, url, e.getMessage());
        }
    }

    private static void closeHiveConnection() {
        try {
            con.close();
            logger.debug("Hive connection closed");
        } catch (SQLException e) {
            logger.warn("Unable to close Hive connection: " + e.getMessage());
        }
    }
}
