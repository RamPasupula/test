
package com.uob.edag.scanners;

import java.io.File;

/**
 * @author cs186076
 */
public interface ScannerCallback {
    void addFile(File f);
    void addOffset(Long l);
}
