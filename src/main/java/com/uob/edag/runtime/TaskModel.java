package com.uob.edag.runtime;

import com.google.common.base.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import javax.ws.rs.GET;
import java.io.File;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Created by cs186076 on 24/5/17.
 */
@AllArgsConstructor
public class TaskModel {
    @Getter @Setter private List<Step> steps;

    @Getter @Setter File source;

    // is a delimited file or a fixed width file.
    @Getter @Setter boolean isDelimitedFile;

    // These are the list of fixed widths for which column conversion needs to be done.
    @Getter @Setter
    Optional<String> fixedWidths;

    @Getter @Setter String charset;

    @Getter @Setter boolean inCompressed;

    @Getter @Setter File destination;

    @Getter @Setter boolean outCompressed;

    @Getter @Setter StandardOpenOption [] modes;
}
