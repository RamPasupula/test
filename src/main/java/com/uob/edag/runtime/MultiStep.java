package com.uob.edag.runtime;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.concurrent.Future;

/**
 * Created by cs186076 on 18/5/17.
 */
public class MultiStep extends AbstractStep<String> {

    @Setter @Getter private List<Step> steps;

    @Override
    public String performStep(String record) {
        return null;
    }

}
