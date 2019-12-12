package com.uob.edag.model;


import com.google.common.base.Optional;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by cs186076 on 27/5/17.
 */
public class AbstractFileModel {

    @Getter @Setter boolean isDelimitedFile;

    @Getter @Setter
    Optional<String> fixedWidths;

    @Getter @Setter String charset;
}
