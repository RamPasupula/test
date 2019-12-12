package com.uob.edag.scanners;

import com.uob.edag.model.FileModel;

import java.util.concurrent.Future;

/**
 * Created by cs186076 on 17/5/17.
 */
public interface Producer {

        public void start() throws Exception;
        public void stop() throws Exception;
        public Future<Integer> run(FileModel f) throws Exception;
}
