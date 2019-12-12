
package com.uob.edag.runtime;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;

import com.uob.edag.exception.EDAGException;

/**
 * @author cs186076
 */
public class PreprocessorCli {

    boolean init = false;
    static Options options = new Options();
    /**
     * Once the files are received on the landing server, we run sequence of codes, to automatically process the data.
     * -f stands for the yaml file name.
     * @param arguments
     */
    public static void main (String [] arguments){
        String configFile = null;
        boolean resetState = false;
        String bizDate = null;
        CommandLineParser parser = new DefaultParser();
        try {
            options.addOption("h", "help", false, "Show Help");
            options.addOption("f", "fileName", true, "File Name");
            options.addOption("r", "resetState", true, "Reset State");
            options.addOption("b", "bizDate", true, "Business Date");
            CommandLine command = parser.parse(options, arguments);
            if (command.hasOption("h")) {
                //showHelp();
            }
            if (command.hasOption("f")) {
                configFile = command.getOptionValue("f");
            }
            if (command.hasOption("r")) {
                resetState = Boolean.valueOf(command.getOptionValue("r"));
            }
            if (command.hasOption("b")) {
                bizDate = command.getOptionValue("b");
            }
            PreprocessorCli cli = new PreprocessorCli();
            cli.run(configFile,resetState,bizDate);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
    private Config getConfig(String configFile, boolean resetState, String processID, String countryCode, String bizDate) throws EDAGException {
    	Path currentRelativePath = Paths.get("");
      Path cwd = currentRelativePath.toAbsolutePath();
      try {
        FileSystemManager fsm = VFS.getManager();
        Config config = ConfigReader.readConfig(new FileInputStream(configFile));
        config.setResetState(resetState);
        if(config.isResetState()){
            fsm.resolveFile(cwd.toFile(),config.getStateFile()).delete();
        }
        
        config.setBizDate(bizDate);
  			config.setProcessID(processID);
  			config.setCountryCode(countryCode);
        
        return config;
      } catch (Exception e) {
      	throw new EDAGException(e.getMessage());
      }	
    }
    
    public void run(String configFile, boolean resetState, String processID, String countryCode, String bizDate) throws EDAGException {
    	try {
    		Config config = getConfig(configFile, resetState, processID, countryCode, bizDate);
    		Runner runner = new Runner(config);
    		runner.run();
    	} catch (Exception e) {
    		throw new EDAGException(e.getMessage());
    	}
    }

    public void run(String configFile,boolean resetState, String bizDate) throws EDAGException {
        run(configFile, resetState, null, null, bizDate);
    }
}