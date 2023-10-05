/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.parsers.FileNameInfos;
import com.powsybl.caseserver.parsers.FileNameParser;
import com.powsybl.caseserver.parsers.FileNameParsers;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.Network;
import org.apache.commons.lang3.Functions;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public interface CaseService {
    default void validateCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        if (!caseName.matches("[^<>:\"/|?*]+(\\.[\\w]+)")) {
            throw CaseException.createIllegalCaseName(caseName);
        }
    }

    default CaseInfos createInfos(String fileBaseName, UUID caseUuid, String format) {
        FileNameParser parser = FileNameParsers.findParser(fileBaseName);
        if (parser != null) {
            Optional<? extends FileNameInfos> fileNameInfos = parser.parse(fileBaseName);
            if (fileNameInfos.isPresent()) {
                return CaseInfos.create(fileBaseName, caseUuid, format, fileNameInfos.get());
            }
        }
        return CaseInfos.builder().name(fileBaseName).uuid(caseUuid).format(format).build();
    }

    List<CaseInfos> getCases();

    boolean caseExists(UUID caseUuid);

    CaseInfos getCase(UUID caseUuid);

    String getFormat(UUID caseUuid);

    String getCaseName(UUID caseUuid);

    Optional<Network> loadNetwork(UUID caseUuid);

    Optional<byte[]> getCaseBytes(UUID caseUuid);

    UUID importCase(MultipartFile file, boolean withExpiration);

    UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration);

    void disableCaseExpiration(UUID caseUuid);

    void deleteCase(UUID caseUuid);

    void deleteAllCases();

    List<CaseInfos> searchCases(String query);

    void reindexAllCases();

    List<CaseInfos> getMetadata(List<UUID> ids);

    void setComputationManager(ComputationManager mock);

    <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, Functions.FailableFunction<Path, R, T> f);

    void setFileSystem(FileSystem fileSystem);

    Path getCaseFile(UUID caseUuid);

    void checkStorageInitialization();
}
