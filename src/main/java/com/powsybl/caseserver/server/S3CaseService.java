package com.powsybl.caseserver.server;

import org.apache.commons.lang3.Functions;

import java.nio.file.Path;
import java.util.UUID;

public interface S3CaseService extends CaseService {
    <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, Functions.FailableFunction<Path, R, T> f);
}
