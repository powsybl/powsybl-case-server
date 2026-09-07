package com.powsybl.caseserver.dto;

import java.io.InputStream;

public record CaseStream(
    InputStream inputStream,
    long contentLength
) { }
