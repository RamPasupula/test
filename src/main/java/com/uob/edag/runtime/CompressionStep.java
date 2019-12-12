package com.uob.edag.runtime;

import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * Created by cs186076 on 24/5/17.
 */
public class CompressionStep extends AbstractStep<File>{

    @Getter @Setter private String previousFolder;
    @Getter @Setter private String compressionPrefix;

    @Override
    public File performStep(File recordFile) {
        try {
            FileInputStream fis = new FileInputStream(recordFile);
            File outFile = computeDestinationFile(recordFile,previousFolder,compressionPrefix);
            FileOutputStream fos = new FileOutputStream(outFile);
            GZIPOutputStream gzipWriter = new GZIPOutputStream(fos);
            byte[] buffer = new byte[1024];
            int len;
            while((len=fis.read(buffer)) != -1){
                gzipWriter.write(buffer, 0, len);
            }
            gzipWriter.close();
            fos.close();
            fis.close();
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return recordFile;
    }
}
