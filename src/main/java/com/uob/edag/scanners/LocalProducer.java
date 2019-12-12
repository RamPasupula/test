package com.uob.edag.scanners;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.uob.edag.model.FileModel;
import com.uob.edag.queue.TaskExecutor;
import com.uob.edag.runtime.Step;
import com.uob.edag.runtime.Task;
import com.uob.edag.runtime.Job;
import com.uob.edag.runtime.TaskModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by cs186076 on 18/5/17.
 */
public class LocalProducer implements Producer {

    static final Logger logger = LoggerFactory.getLogger(LocalProducer.class);

    @Getter
    @Setter
    Map parameters = new HashMap();
    @Getter
    @Setter
    boolean compressed_in = false;
    @Getter
    @Setter
    boolean compressed_out = false;
    @Getter
    @Setter
    Job job;
    @Getter
    @Setter
    Integer executorCount = 10;
    List<Step> steps = null;
    TaskExecutor taskExecutor = null;
    AtomicInteger fileCount = new AtomicInteger(0);

    @Override
    public void start() throws Exception {
        taskExecutor = new TaskExecutor(executorCount);
        taskExecutor.startAsync();
        steps = job.getMultistep().getSteps();
        logger.info("Starting the LocalProducer here ");
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping the LocalProducer here ");
        taskExecutor.stopAsync();
    }

    @Override
    public Future<Integer> run(FileModel fileModel) throws Exception {
        fileCount.incrementAndGet();
        File inputFile = new File(fileModel.getSourceDirectory());
        File destFile = computeDestinationFile(inputFile, job.getDestinationFilePrefix());

        TaskModel tm = new TaskModel(steps, inputFile, fileModel.isDelimitedFile(), fileModel.getFixedWidths(), fileModel.getCharset(), compressed_in, destFile, compressed_out,
                new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE});
        Task t = new Task(tm);
        return taskExecutor.submit(t);
    }

    private File computeDestinationFile(File sourceFile, String destinationPrefix) throws IOException {
        String sourceParent = sourceFile.getParent();
        String sourceFileName = sourceFile.getName();
        Path destinationPath = Paths.get(sourceParent, sourceFileName+destinationPrefix);
        if (!destinationPath.toFile().exists()) {
            Files.createFile(destinationPath);
        }
        return destinationPath.toFile();
    }
}
