package com.uob.edag.runtime;

import org.junit.Test;

/**
 * Created by cs186076 on 23/5/17.
 */
public class PreProcessorCliTest {

    @Test
    public void testPreprocessCli(){
        String [] args = new String[]{"-f=conf/step-replacer.yaml"};
        PreprocessorCli.main(args);
    }
}
