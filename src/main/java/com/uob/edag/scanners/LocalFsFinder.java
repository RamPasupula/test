package com.uob.edag.scanners;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.uob.edag.model.FileModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author cs186076
 */
public class LocalFsFinder extends SimpleFileVisitor<Path> {

    static final Logger logger = LoggerFactory.getLogger(LocalFsFinder.class);

    public static interface LocalFsFinderCallback {
        void notifyFile(Path path);
    }

    LocalFsFinderCallback cb;
    Set state;
    Path folderPath;
    List<Pattern> globs;
    Map<String,FileModel> models;

    public LocalFsFinder(String basePath, List<Pattern> globs, Map<String,FileModel> models,Set state, LocalFsFinderCallback cb) {
        this.cb = cb;
        this.state = state;
        this.globs = globs;
        folderPath = Paths.get(basePath);
        this.models = models;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (accept(file)) {
            state.add(file.toAbsolutePath().toString());
            cb.notifyFile(file.toAbsolutePath());
        }
        return FileVisitResult.CONTINUE;
    }

    boolean accept(Path entry) throws IOException {
        Path relPath = folderPath.relativize(entry);;
        if (state.contains(entry.toAbsolutePath().toString())) {
            logger.info(String.format("File %s skipped (already processed)", entry.toAbsolutePath().toString()));
            return false;
        }
        if(models.containsKey(folderPath.toString())) {
            if (globs == null) {
                logger.debug(String.format("File %s accepted", entry.toAbsolutePath().toString()));
                return true;
            }
            for (Pattern m : globs) {
                if (m.matcher(relPath.toString()).matches()) {
                    logger.debug(String.format("File %s accepted", entry.toAbsolutePath().toString()));
                    return true;
                }
            }
        }
        logger.debug(String.format("File %s skipped (not matching)", entry.toAbsolutePath().toString()));
        return false;
    }
}
