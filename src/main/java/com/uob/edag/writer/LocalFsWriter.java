package com.uob.edag.writer;

import com.uob.edag.runtime.TaskModel;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by cs186076 on 22/5/17.
 */
public class LocalFsWriter<String> extends AbstractWriter{

    private static Logger logger = Logger.getLogger(LocalFsWriter.class);

    private AsynchronousFileChannel fileChannel;
    private AtomicInteger atmi;
    private BufferedWriter bw;
    private java.lang.String charset;

    public LocalFsWriter(TaskModel taskModel) throws IOException {
        Path path = Paths.get(taskModel.getDestination().toURI());
        fileChannel = AsynchronousFileChannel.open(path,taskModel.getModes());
        atmi = new AtomicInteger(0);
        this.charset = taskModel.getCharset();
        bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(taskModel.getDestination(),true),this.charset));
    }

    @Override
    public void write(Object record) throws Exception {
        Future<Integer> operation = null;
        try {
            /*byte[] bytw = record.toString().getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(bytw.length);
            long position = atmi.get()>0?atmi.get()-1:0;
            buffer.put(bytw);
            buffer.flip();
            fileChannel.write(buffer, position, buffer, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    atmi.getAndSet(result);
                    //logger.info("Buffer Write Completed");
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    logger.error("Buffer Write Failed");
                }
            });
            buffer.clear();*/
            java.lang.String hexRecord = (java.lang.String)record;
            byte[] bytes = Hex.decodeHex(hexRecord.toCharArray());
            java.lang.String actualRecord = new java.lang.String(bytes,this.charset);
            bw.write(actualRecord);
            bw.newLine();
            atmi.incrementAndGet();
            if(atmi.get() % 100 == 0) bw.flush();
        }catch(Exception e ){
            throw e;
        }finally{
            bw.flush();
        }
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
        bw.close();
    }
}
