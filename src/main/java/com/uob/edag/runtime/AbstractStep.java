package com.uob.edag.runtime;

import lombok.Getter;
import lombok.Setter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by cs186076 on 18/5/17.
 */
public abstract class AbstractStep<T> implements Step<T>{

    @Getter @Setter private Integer stepId;
    @Getter @Setter private String stepName;
    @Getter  private String fixedWidths;
    @Getter  private boolean isDelimitedFile;

    public void setFixedWidths(String fixedWidths){
        this.fixedWidths = fixedWidths;
    }

    public void setDelimitedFile(boolean delimited){
        this.isDelimitedFile = delimited;
    }

    protected File computeDestinationFile(File sourceFile, String destinationFolder, String destinationPrefix) throws IOException {
        String sourceParent = sourceFile.getParent();
        String sourceBase = sourceFile.getParentFile().getParent();
        String sourceFileName = sourceFile.getName();
        Path destinationPath = Paths.get(sourceBase,sourceParent,destinationFolder,sourceFileName+"."+destinationPrefix);
        if(!destinationPath.toFile().exists()){
            Files.createFile(destinationPath);
        };
        return destinationPath.toFile();
    }

    @Override
    public abstract T performStep(T record) throws IOException;
}
