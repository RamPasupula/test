package com.uob.edag.utils;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.List;

/**
 * Created by cs186076 on 15/5/17.
 */
public class ExecutorsUtils {

        static final Logger logger1 = Logger.getLogger(ExecutorsUtils.class);


        public static final long EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = 60;
        public static final TimeUnit EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
        private static final ThreadFactory DEFAULT_THREAD_FACTORY =
                newThreadFactory(Optional.<Logger>absent());

        /**
         * Get a default {@link java.util.concurrent.ThreadFactory}.
         *
         * @return the default {@link java.util.concurrent.ThreadFactory}
         */
        public static ThreadFactory defaultThreadFactory() {
            return DEFAULT_THREAD_FACTORY;
        }

        /**
         * Get a new {@link java.util.concurrent.ThreadFactory} that uses a  to handle uncaught exceptions.
         *
         * @param logger an {@link com.google.common.base.Optional} wrapping the {@link
         *               org.slf4j.Logger} that uses to log
         *               uncaught exceptions thrown in threads
         * @return a new {@link java.util.concurrent.ThreadFactory}
         */
        public static ThreadFactory newThreadFactory(Optional<Logger> logger) {
            return newThreadFactory(logger, Optional.<String>absent());
        }

        /**
         * Get a new  java.util.concurrent.ThreadFactory} that uses to handle uncaught exceptions and the given thread name
         * format.
         */
        public static ThreadFactory newThreadFactory(Optional<Logger> logger,
                                                     Optional<String> nameFormat) {
            return newThreadFactory(new ThreadFactoryBuilder(), logger, nameFormat);
        }

        /**
         * Get a new {@link ThreadFactory} that uses to handle
         * uncaught exceptions, uses the given thread name format, and produces daemon threads.
         *
         * @param logger     an {@link Optional} wrapping the {@link Logger} that  uses to log uncaught exceptions thrown in
         *                   threads
         * @param nameFormat an {@link Optional} wrapping a thread naming format
         * @return a new {@link ThreadFactory}
         */
        public static ThreadFactory newDaemonThreadFactory(Optional<Logger> logger,
                                                           Optional<String> nameFormat) {
            return newThreadFactory(new ThreadFactoryBuilder().setDaemon(true), logger, nameFormat);
        }

        private static ThreadFactory newThreadFactory(ThreadFactoryBuilder builder,
                                                      Optional<Logger> logger, Optional<String> nameFormat) {
            if (nameFormat.isPresent()) {
                builder.setNameFormat(nameFormat.get());
            }
            return builder.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger1.error(String.format("Thread %s threw an uncaught exception: %s", t, e), e);
                }
            }).build();
        }

        /**
         * Shutdown an {@link ExecutorService} gradually, first disabling new task submissions and later
         * cancelling existing tasks.
         *
         * <p> The implementation is based on the implementation of Guava's
         * MoreExecutors.shutdownAndAwaitTermination, which is available since version 17.0. We cannot
         * use Guava version 17.0 or after directly, however, as it cannot be used with Hadoop 2.6.0 or
         * after due to the issue reported in HADOOP-10961. </p>
         *
         * @param executorService the {@link ExecutorService} to shutdown
         * @param logger          an {@link Optional} wrapping the {@link Logger} that is used to log
         *                        metadata of the executorService if it cannot shutdown all its threads
         * @param timeout         the maximum time to wait for the {@code ExecutorService} to terminate
         * @param unit            the time unit of the timeout argument
         */
        public static void shutdownExecutorService(ExecutorService executorService,
                                                   Optional<Logger> logger, long timeout, TimeUnit unit) {
            Preconditions.checkNotNull(unit);
            // Disable new tasks from being submitted
            executorService.shutdown();
            //if (logger.isPresent()) {
            logger.get().info("Attempting to shutdown ExecutorService: " + executorService);
            //}
            try {
                long halfTimeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit) / 2;
                // Wait for half the duration of the timeout for existing tasks to terminate
                if (!executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                    // Cancel currently executing tasks
                    executorService.shutdownNow();


                    // Wait the other half of the timeout for tasks to respond to being cancelled
                    if (!executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)
                            && logger.isPresent()) {
                        logger.get()
                                .error("Could not shutdown all threads in ExecutorService: " + executorService);
                    }
                }
            } catch (InterruptedException ie) {
                // Preserve interrupt status
                Thread.currentThread().interrupt();
                // (Re-)Cancel if current thread also interrupted
                executorService.shutdownNow();

                // if (logger.isPresent()) {
                logger.get().info("Attempting to shutdownNow ExecutorService: " + executorService);
                //}
            }
        }

        /**
         * Shutdown an {@link ExecutorService} gradually, first disabling new task submissions and later
         * cancelling existing tasks.
         *
         * <p> This method calls {@link #shutdownExecutorService(ExecutorService, Optional, long,
         * TimeUnit)} with default timeout time {@link #EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT} and time unit
         * {@link #EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT}. </p>
         *
         * @param executorService the {@link ExecutorService} to shutdown
         * @param logger          an {@link Optional} wrapping a {@link Logger} to be used during
         *                        shutdown
         */
        public static void shutdownExecutorService(ExecutorService executorService,
                                                   Optional<Logger> logger) {
            shutdownExecutorService(executorService, logger, EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT,
                    EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT);
        }

        public static <F, T> List<T> parallelize(final List<F> list, final Function<F, T> function,
                                                 int threadCount, int timeoutInSecs, Optional<Logger> logger) throws ExecutionException {

            Preconditions.checkArgument(list != null, "Input list can not be null");
            Preconditions.checkArgument(function != null, "Function can not be null");

            final List<T> results = Lists.newArrayListWithCapacity(list.size());
            List<Future<T>> futures = Lists.newArrayListWithCapacity(list.size());

            ExecutorService executorService = MoreExecutors
                    .getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount,
                            ExecutorsUtils.newThreadFactory(logger)), 2, TimeUnit.MINUTES);

            for (final F l : list) {
                futures.add(executorService.submit(new Callable<T>() {
                    @Override
                    public T call() throws Exception {
                        return function.apply(l);
                    }
                }));
            }

            ExecutorsUtils.shutdownExecutorService(executorService, logger, timeoutInSecs,
                    TimeUnit.SECONDS);

            for (Future<T> future : futures) {
                try {
                    results.add(future.get());
                } catch (InterruptedException e) {
                    throw new ExecutionException("Thread interrupted", e);
                }
            }
            return results;
        }
}
