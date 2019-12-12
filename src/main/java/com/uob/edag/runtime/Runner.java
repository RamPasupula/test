/**
 * Copyright © DataSpark Pte Ltd 2014 - 2015.
 * This software and any related documentation contain confidential and proprietary information of DataSpark and its licensors (if any).
 * Use of this software and any related documentation is governed by the terms of your written agreement with DataSpark.
 * You may not use, download or install this software or any related documentation without obtaining an appropriate licence agreement from DataSpark.
 * All rights reserved.
 */
package com.uob.edag.runtime;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.uob.edag.model.FileModel;
import com.uob.edag.scanners.Scanner;
import com.uob.edag.scanners.ScannerCallback;

/**
 * @author cs186076
 */
public class Runner implements ScannerCallback {

    static final Logger logger = Logger.getLogger(Runner.class);

    Config config;
    Set<String> running = new HashSet<>();
    ThreadPoolExecutor producersExecutor;
    LinkedBlockingQueue<Runnable> producersQueue;
    Date startDate;
    Date endDate;
    static AtomicLong processedTasks = new AtomicLong(0);
    static AtomicLong startTime;

    static {
        ScheduledExecutorService monitor1 = Executors.newSingleThreadScheduledExecutor();
        monitor1.scheduleAtFixedRate(new Runnable() {
            long prev = 0L;

            public void run() {
                long cur = processedTasks.get();
                //long val = cur - this.prev;
                //if (val != 0L) {
                logger.info(cur
                        - this.prev
                        + " lines in "
                        + (System.currentTimeMillis() - startTime
                        .getAndSet(System.currentTimeMillis())));
                //}
                this.prev = cur;
            }
        }, 30L, 10L, TimeUnit.SECONDS);
    }

    public Runner(Config config) {
        this.config = config;
        // this.startDate = startDate;
        // this.endDate = endDate;
        producersQueue = new LinkedBlockingQueue<Runnable>();
        producersExecutor = new ThreadPoolExecutor(
                config.getProducers(), config.getProducers(),
                1000, TimeUnit.MINUTES,
                producersQueue
        );
    }

    public void run() throws Exception {
        config.getProducer().start();
        Scanner fps = config.getScanner();
        Map<String, FileModel> listFileModel = fps.run(this, config);
        List<Future<Integer>> ll = new ArrayList<Future<Integer>>();
        for (String s : running) {
            ll.add(schedule(listFileModel.get(s)));
        }
        try {
            for (Future<Integer> f : ll) {
                f.get();
            }
        }finally{
            //producersExecutor.shutdown();
            producersExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            config.getProducer().stop();
        }
    }

    public void runMessages() throws Exception {
        config.getProducer().start();
        config.getScanner().run(this, config.getProducer());
    }

    Future<Integer> schedule(final FileModel fileModel) {
        return producersExecutor.submit(new Callable<Integer>() {
            @Override
            public Integer call() {
                try {
                    logger.info(String.format("Processing file: %s (%d/%d)", fileModel.toString(), processedTasks.incrementAndGet(), running.size()));
                    Future<Integer> ff = config.getProducer().run(fileModel);
                    while(ff.isDone()){
                        return ff.get();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("waiting to complete");
                }
                return -1;
            }
        });
    }

    @Override
    public void addFile(final File f) {
        logger.debug("Adding file " + f.toString());
        running.add(f.getAbsolutePath());
    }

    @Override
    public void addOffset(Long l) {
        processedTasks.set(l);
    }
}
