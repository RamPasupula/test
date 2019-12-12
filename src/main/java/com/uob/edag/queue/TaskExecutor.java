package com.uob.edag.queue;

import java.util.concurrent.*;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.uob.edag.model.FileModel;
import com.uob.edag.runtime.Task;
import com.uob.edag.utils.ExecutorsUtils;
import com.uob.edag.utils.PlatformUtils;
import org.apache.log4j.Logger;

/**
 * Created by cs186076 on 15/5/17.
 */
public class TaskExecutor extends AbstractIdleService{

    static final Logger log = Logger.getLogger(PlatformUtils.class);

    private final ExecutorService fileExecutor;

    /**
     * Constructor used internally.
     */
    public TaskExecutor(int fileExecutorThreadPoolSize) {
        Preconditions.checkArgument(fileExecutorThreadPoolSize > 0,
                "File executor thread pool size should be positive");
        // Currently a fixed-size thread pool is used to execute files. We probably need to revisit this later.
        this.fileExecutor = Executors.newFixedThreadPool(fileExecutorThreadPoolSize,
                ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("ExecutorService-%d")));
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Starting the file executor");
        if ((this.fileExecutor.isShutdown() || this.fileExecutor.isTerminated())) {
            throw new IllegalStateException("file thread pool executor is shutdown or terminated");
        }
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Stopping the file executor");
        try {
            ExecutorsUtils.shutdownExecutorService(this.fileExecutor, Optional.of(log));
        } finally {
        }
    }

    public Future<Integer> submit(Task task) throws ExecutionException, InterruptedException {
        Future<Integer> fI = this.fileExecutor.submit(task);
        return fI;
    }

    public Future<FileModel> call(Callable task) throws ExecutionException, InterruptedException {
         return this.fileExecutor.submit(task);
    }
}