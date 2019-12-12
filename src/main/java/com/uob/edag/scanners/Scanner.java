
package com.uob.edag.scanners;

import com.uob.edag.exception.EDAGException;
import com.uob.edag.model.FileModel;
import com.uob.edag.runtime.Config;

import java.util.Map;

/**
 * @author cs186076
 */
public interface Scanner {

    Map<String, FileModel> run(ScannerCallback cb, Config config) throws EDAGException;
    void run() throws EDAGException;
    void run(ScannerCallback runner, Producer producer) throws EDAGException;
}
