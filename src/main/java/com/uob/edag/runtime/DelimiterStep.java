package com.uob.edag.runtime;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.io.StringReader;
/**
 * Created by cs186076 on 26/5/17.
 */
public class DelimiterStep extends AbstractStep<String> {

    @Getter @Setter java.lang.String fixedWidthsDelimiter;
    @Getter @Setter java.lang.String appendDelimiter;

    @Override
    public String performStep(String record) throws IOException {
        java.lang.String rec = (java.lang.String) record;
        StringReader r = new StringReader(rec);
        StringBuilder sb = new StringBuilder();
        for (java.lang.String len : getFixedWidths().split(fixedWidthsDelimiter)) {
            char[] buf = new char[Integer.parseInt(len)];
            int read = r.read(buf);
            if (read != buf.length)
                throw new IllegalArgumentException();
            sb.append(new java.lang.String(buf, 0, read)).append(appendDelimiter);
        }
        return sb.toString();
    }
}
