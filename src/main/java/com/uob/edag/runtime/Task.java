package com.uob.edag.runtime;

import com.google.common.io.Closer;
import com.uob.edag.writer.LocalFsWriter;
import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cs186076 on 18/5/17.
 */
public class Task<String> implements Callable<Integer> {

    static final Logger logger = LoggerFactory.getLogger(Task.class);

    private AtomicInteger lineCount = new AtomicInteger(0);
    private AtomicInteger bytesCount = new AtomicInteger(0);
    private Closer closer;
    private com.uob.edag.writer.Writer writer;
    private TaskModel taskModel;

    public Task(TaskModel taskModel) throws IOException {
        this.taskModel = taskModel;
        this.closer = Closer.create();
        this.writer = this.closer.register(new LocalFsWriter<java.lang.String>(this.taskModel));
    }

    @Override
    public Integer call() {
        try{
            InputStream is = new FileInputStream(taskModel.getSource());
            if (taskModel.isInCompressed())
                is = new ZCompressorInputStream(is);
            InputStreamReader isr = new InputStreamReader(is, taskModel.getCharset());
            BufferedReader reader = new BufferedReader(isr);
            java.lang.String line = null;
            List<Future<?>> futureList = new LinkedList<Future<?>>();
            while ((line = reader.readLine()) != null) {
                java.lang.String rec =  line;
                //logger.info(rec);
                for(Step step : taskModel.getSteps()){
                    rec =  (java.lang.String) step.performStep(rec);
                }
                this.writer.write(rec);
                lineCount.incrementAndGet();
            }
            logger.info("The linecounter for the lines :"+lineCount);
        }catch(Exception e ){
            e.printStackTrace();
        }finally{
            try {
                this.writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return lineCount.get();
    }
}
