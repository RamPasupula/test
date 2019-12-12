package com.uob.edag.writer;


import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Created by cs186076 on 22/5/17.
 */
public abstract class AbstractWriter<T> implements Writer<T> {

    @Getter @Setter private String path;

    @Override
    public abstract void write(T record) throws  Exception;
    @Override
    public abstract void close() throws IOException;
}
