package com.uob.edag.utils;

import org.apache.log4j.Logger;
import com.uob.edag.queue.BoundedBlockingRecordQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by cs186076 on 15/5/17.
 */
public class PlatformUtils {

    static final Logger logger = Logger.getLogger(PlatformUtils.class);

    public PlatformUtils(){

    }

    public BoundedBlockingRecordQueue createQueue(){
        return BoundedBlockingRecordQueue.newBuilder()
                .hasCapacity(Double.valueOf(Math.pow(2, 20)).intValue())
                .useTimeout(100000L)
                .useTimeoutTimeUnit(TimeUnit.MILLISECONDS)
                .collectStats()
                .build();
    }
}
