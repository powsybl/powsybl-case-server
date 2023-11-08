package com.powsybl.caseserver.server;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.UUID;

public interface FsCaseService extends CaseService {

    void setFileSystem(FileSystem fileSystem);

    Path getCaseFile(UUID caseUuid);

    void checkStorageInitialization();
}
