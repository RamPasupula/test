package com.uob.edag.scanners;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.uob.edag.constants.UobConstants;
import com.uob.edag.dao.IngestionDao;
import com.uob.edag.exception.EDAGException;
import com.uob.edag.exception.EDAGIOException;
import com.uob.edag.model.FieldModel;
import com.uob.edag.model.FileModel;
import com.uob.edag.model.HadoopModel;
import com.uob.edag.model.ProcessModel;
import com.uob.edag.processor.BaseProcessor;
import com.uob.edag.runtime.Config;
import com.uob.edag.runtime.Job;
import com.uob.edag.utils.FileUtility;
import com.uob.edag.utils.FileUtility.OperationType;
import com.uob.edag.utils.FileUtilityFactory;
import com.uob.edag.utils.PropertyLoader;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by cs186076 on 17/5/17.
 */
public class FilePatternScanner implements Scanner, LocalFsFinder.LocalFsFinderCallback {

    final static long MILLISECONDS_IN_A_DAY = 86400000;
    static final Logger logger = Logger.getLogger(FilePatternScanner.class);

    @Getter
    @Setter
    Date startDate = new Date(Long.MIN_VALUE);
    @Getter
    @Setter
    String path;
    @Getter
    @Setter
    List<Pattern> patterns;
    @Getter
    @Setter
    Job pre;

    Set<String> state;
    ScannerCallback scannerCallback;
    @Getter
    @Setter
    Pattern sourceFormat;
    Cache<Path, Set<String>> ctCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
    
    Set<String> getSourceFilePaths(Path p) throws EDAGIOException {

        Set<String> res = ctCache.getIfPresent(p);
        if (res != null) return res;

        res = new HashSet<>();
        try {
	        DirectoryStream<Path> subDirs = Files.newDirectoryStream(p);
	        for (Path subDir : subDirs) {
	            try (DirectoryStream<Path> sub_Sub_Dirs = Files.isDirectory(subDir) ? Files.newDirectoryStream(subDir) : null) {
		            if (null == sub_Sub_Dirs) {
		                continue;
		            }
		            for (Path subSubDir : sub_Sub_Dirs) {
		                try (DirectoryStream<Path> sub_Sub_SUB_Dirs = Files.isDirectory(subSubDir) ? Files.newDirectoryStream(subSubDir) : null) {
			                if (null == sub_Sub_SUB_Dirs) {
			                    continue;
			                }
			                for(Path ssssFile : sub_Sub_SUB_Dirs) {
			                    Path relativePath = p.relativize(ssssFile);
			                    logger.info(relativePath.toString());
			                    if (sourceFormat == null || sourceFormat.matcher(relativePath.toString()).matches()) {
			                        res.add(relativePath.toString());
			                        logger.info("matched:: " + relativePath.toString());
			                    }
			                }
		                }
		            }
	            }
	        }
        } catch (IOException e) {
        	throw new EDAGIOException(e.getMessage());
        }
        ctCache.put(p, res);
        return res;
    }

    Set<String> loadProcessed(String path) {
        try {
            File f = Paths.get(path).toFile();
            FileInputStream i = new FileInputStream(f);
            BufferedReader r = new BufferedReader(new InputStreamReader(i));
            Set<String> processed = new HashSet<>();
            while (true) {
                String l = r.readLine();
                if (l == null) break;
                processed.add(l);
            }
            r.close();
            i.close();
            return processed;
        } catch (FileNotFoundException e) {
            return new HashSet<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void saveProcessed(Set<String> processed, String path) {
        try {
            File folder = new File(path);
            if (!folder.exists()) folder.mkdirs();
            File f = Paths.get(path).toFile();
            if (f.exists()) f.delete();
            FileOutputStream o = new FileOutputStream(f, false);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(o));
            for (String s : processed) {
                writer.write(s);
                writer.newLine();
            }
            writer.flush();
            o.flush();
            writer.close();
            o.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    ConcurrentHashMap<String, FileModel> getAndMoveSourceFilePathsFromMetadata(String processID, String countryCode, String bizDate) throws EDAGException {
    	  FileUtility fileUtil = FileUtilityFactory.getFileUtility(null, OperationType.Local);
        final ConcurrentHashMap<String, FileModel> listFileModel = new ConcurrentHashMap<>();
        try {
            final IngestionDao dao = new IngestionDao();
            List<ProcessModel> plmList = new ArrayList<ProcessModel>();
            if (processID == null) {
            	plmList.addAll(dao.retrieveProcessList());
            } else {
            	plmList.add(dao.retrieveProcessMaster(processID));
            }
            
            //te = new TaskExecutor(plmList.size());// defaulting the size of the list to plmList Size.
            //te.startAsync();
            final BaseProcessor bp = new BaseProcessor();
            for (final ProcessModel procModel : plmList) {
                List<String> cntryCodes = new ArrayList<String>();
                if (countryCode == null) {
                	cntryCodes.addAll(dao.retrieveProcessCountry(procModel.getProcId()).keySet());
                } else {
                	cntryCodes.add(countryCode);
                }
                
                for (final String cntryCode : cntryCodes) {
                    FileModel fileModel = null;
                    try {
                        //bizDate = dao.getBizDate(cntryCode, procModel.getProcId());
                        final ProcessModel fullProcessModel = bp.retrieveMetadata(procModel.getProcId(), cntryCode);
                        fileModel = fullProcessModel.getSrcInfo();
                        String newSrcFileLoc = PropertyLoader.getProperty(UobConstants.LANDING_AREA_PROCESSING_DIR_PATH);
                        String environment = PropertyLoader.getProperty(UobConstants.ENVIRONMENT);
                        String environmentNum = PropertyLoader.getProperty(UobConstants.ENVIRONMENT_NUM);
                        String charset = PropertyLoader.getProperty(cntryCode + "_CHARSET");
                        charset = StringUtils.isNotEmpty(charset) ? charset : PropertyLoader.getProperty(UobConstants.DEFAULT_CHARSET);
                        logger.debug("Charset passed to command is:" + charset);
                        fileModel.setCharset(charset);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                        String currentTime = sdf.format(System.currentTimeMillis());
                        String dataDir = PropertyLoader.getProperty(UobConstants.DATADIR);
                        newSrcFileLoc = newSrcFileLoc.replace(UobConstants.DATADIR_STR_PARAM, dataDir);
                        newSrcFileLoc = newSrcFileLoc.replace(UobConstants.SRC_SYS_PARAM,
                                fullProcessModel.getSrcSystemId().toLowerCase()).replace(UobConstants.ENV_PARAM, environment)
                                .replace(UobConstants.ENV_NUM_PARAM, environmentNum)
                                .replace(UobConstants.FREQ_PARAM, fullProcessModel.getProcFreq())
                                .replace(UobConstants.FILENM_PARAM, fileModel.getSourceFileName())
                                .replace(UobConstants.SYS_TS_PARAM, currentTime)
                                .replace(UobConstants.COUNTRY_PARAM, cntryCode.toLowerCase())
                                .replace(UobConstants.BIZ_DATE_PARAM, bizDate);

                        logger.info("Going to move the file " + fileModel.getSourceDirectory()
                                + " to Processing folder:" + newSrcFileLoc);

                        final String srcdir = fileModel.getSourceDirectory()
                                .replace(UobConstants.DATADIR_STR_PARAM, dataDir)
                                .replace(UobConstants.COUNTRY_PARAM, cntryCode.toLowerCase())
                                .trim();
                        final String srcArcDir = fileModel.getSourceArchivalDir()
                                .replace(UobConstants.DATADIR_STR_PARAM, dataDir)
                                .replace(UobConstants.COUNTRY_PARAM, cntryCode.toLowerCase())
                                .replace(UobConstants.BIZ_DATE_PARAM, bizDate);
                        final String newSrcFileLocFinal = newSrcFileLoc;

                        final FileModel finalFileModel = fileModel;
                        /*Callable codeR = new Callable<FileModel>() {
                            @Override
                            public FileModel call() throws Exception {
                                //Steps to do archival and moving files
                                File src = new File(srcdir);
                                File srcFileFinal = new File(newSrcFileLocFinal);
                                File srcArch = new File(srcArcDir);
                                try {
                                    logger.info("srcdir ::" + srcdir + "arc::" + srcArcDir);
                                    FileUtils.archiveFile(src, srcArch);
                                    FileUtils.moveFile(src, srcFileFinal);
                                    finalFileModel.setSourceDirectory(newSrcFileLocFinal);
                                    boolean delimitConversion = checkDelimitedFileConversion(fullProcessModel);
                                    String fixedWidths = getFixedFileWidths(fullProcessModel);
                                    finalFileModel.setDelimitedFile(delimitConversion);
                                    finalFileModel.setFixedWidths(StringUtils.isEmpty(fixedWidths) ? Optional.<String>absent() : Optional.of(fixedWidths));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.info("Unable to archive and move the files");
                                    return null;
                                }
                                while((src.length() == srcFileFinal.length()) && (srcArch.exists())) {
                                    return finalFileModel;
                                }
                                return null;
                            }
                        };
                        Future<FileModel> fis = te.call(codeR);
                        futuresFileModel.add(fis);*/
                        fileUtil.copyFile(srcdir, newSrcFileLocFinal);
                        fileUtil.archiveFile(srcdir, srcArcDir, true);
                        finalFileModel.setSourceDirectory(newSrcFileLocFinal);
                        boolean delimitConversion = checkDelimitedFileConversion(fullProcessModel);
                        String fixedWidths = getFixedFileWidths(fullProcessModel);
                        finalFileModel.setDelimitedFile(delimitConversion);
                        finalFileModel.setFixedWidths(StringUtils.isEmpty(fixedWidths) ? Optional.<String>absent() : Optional.of(fixedWidths));
                        listFileModel.put(finalFileModel.getSourceDirectory(), finalFileModel);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        /*for (Future<FileModel> fi : futuresFileModel) {
            while (fi.isDone()) {
                FileModel fileModel = fi.get();
                listFileModel.put(fileModel.getSourceDirectory(), fileModel);
            }
        }*/
        }finally{
            //te.stopAsync();
        }
        return listFileModel;
    }

    private boolean checkDelimitedFileConversion(ProcessModel procModel) {
        boolean delimitConversion = false;
        FileModel file = (FileModel) procModel.getSrcInfo();
        String layout = file.getSourceFileLayoutCd();
        delimitConversion = UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(layout);
        return delimitConversion;
    }

    private String getFixedFileWidths(ProcessModel procModel) {
        StringBuilder fixedWidths = new StringBuilder();
        FileModel file = (FileModel) procModel.getSrcInfo();
        String layout = file.getSourceFileLayoutCd();
        if (UobConstants.FIXED_FILE.equalsIgnoreCase(layout) || UobConstants.FIXED_TO_DELIMITED_FILE.equalsIgnoreCase(layout)) {
            HadoopModel hadoopModel = (HadoopModel) procModel.getDestInfo();
            Map<Integer, FieldModel> fieldsInfo = hadoopModel.getDestFieldInfo();
            int size = fieldsInfo.size();
            for (int j = 1; j <= size; j++) {
                FieldModel field = fieldsInfo.get(j);
                int colLen = field.getLength();
                if (fixedWidths.length() > 0) {
                    fixedWidths.append(UobConstants.SPACE);
                }
                fixedWidths.append(colLen);
            }
            return fixedWidths.toString();
        }
        return null;
    }

    @Override
    public Map<String, FileModel> run(ScannerCallback cb, Config config) throws EDAGException {
        scannerCallback = cb;
        Map<String, FileModel> llFileModel = getAndMoveSourceFilePathsFromMetadata(config.getProcessID(), config.getCountryCode(), config.getBizDate());
        Set<String> filesList = getSourceFilePaths(Paths.get(path));
        state = loadProcessed(config.getStateFile());
        int fileCount = 1;
        for (String file : filesList) {
            logger.info(String.format("Scanning [File]: [%s (%d/%d)]", file, fileCount, filesList.size()));
            Path currFolder = Paths.get(path, file);
            if (currFolder.toFile().exists()) {
                LocalFsFinder finder = new LocalFsFinder(currFolder.toAbsolutePath().toString(), patterns, llFileModel, state, FilePatternScanner.this);
                Path p;
								try {
									p = Files.walkFileTree(currFolder, finder);
									logger.info(p.toAbsolutePath().toString());
								} catch (IOException e) {
									throw new EDAGIOException(e.getMessage());
								}
            }
            fileCount++;
        }
        saveProcessed(state, config.getStateFile());
        return llFileModel;
    }

    @Override
    public void notifyFile(Path path) {
        state.add(path.toAbsolutePath().toString());
        scannerCallback.addFile(path.toFile());
    }

    @Override
    public void run() throws EDAGException {
        throw new EDAGException("This method hasn't been implemented yet");
    }

    @Override
    public void run(ScannerCallback runner, Producer producer) throws EDAGException {
        throw new EDAGException("This method hasn't been implemented yet");
    }
}
