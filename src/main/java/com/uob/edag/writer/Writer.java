package com.uob.edag.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Created by cs186076 on 22/5/17.
 */
public interface Writer<T> extends Closeable{
    public void write(T record) throws Exception;
}
