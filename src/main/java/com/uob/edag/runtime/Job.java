package com.uob.edag.runtime;

import com.uob.edag.writer.Writer;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

/**
 * Created by cs186076 on 17/5/17.
 */
public class Job {

    private String jobId;
    @Getter @Setter private String jobName;
    @Getter @Setter private MultiStep multistep;
    @Getter @Setter private String destinationFilePrefix;

    public void setJobId(){
        this.jobId = UUID.randomUUID().toString();
    }

    public String getJobId(){
        return this.jobId;
    }
}
