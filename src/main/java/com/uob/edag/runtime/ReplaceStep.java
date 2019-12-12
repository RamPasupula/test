package com.uob.edag.runtime;


import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.List;

/**
 * Created by cs186076 on 18/5/17.
 */
public class ReplaceStep extends AbstractStep<String> {

    private static Logger logger = Logger.getLogger(ReplaceStep.class);

    @Getter
    @Setter
    private Pattern regexp;
    @Getter
    @Setter
    private String replace_pattern;
    @Getter
    @Setter
    private String replace;

    @Override
    public String performStep(final String record) {
        String hex = null;
        try {
            hex = String.format("%040x", new BigInteger(1,record.getBytes("ISO-8859-1")));
            String [] pats = replace_pattern.split(",");
            for(String pat:pats){
                String val = null;
                val = hex.replaceAll(pat,replace);
                hex = val;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return hex;
    }
}
