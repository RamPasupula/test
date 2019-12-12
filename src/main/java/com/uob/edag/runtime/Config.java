package com.uob.edag.runtime;

import com.uob.edag.scanners.Producer;
import com.uob.edag.scanners.Scanner;
import lombok.Getter;
import lombok.Setter;

/**
 * @author cs186076
 */
public class Config {
    @Getter @Setter int sleep = 60*60;
    @Getter @Setter String stateFile;
    @Getter @Setter Scanner scanner;
    @Getter @Setter int producers = 1;
    @Getter @Setter Producer producer;
    @Getter @Setter String name;
    @Getter @Setter String sourceBase;
    // Sleep time between each batch, to slow down producer rate
    @Getter @Setter int batchSleep = 0;
    @Getter @Setter String bizDate;
    @Getter @Setter boolean resetState;
    @Getter @Setter String processID = null;
    @Getter @Setter String countryCode = null;
}
