package com.uob.edag.runtime;

import java.io.IOException;

/**
 * Created by cs186076 on 22/5/17.
 */
public interface Step<T> {

    public void setFixedWidths(String fixedWidths);

    public void setDelimitedFile(boolean delimited);

    public T performStep(T record) throws IOException;

}
