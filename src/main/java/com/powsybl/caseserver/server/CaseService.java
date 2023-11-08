/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.server;

import com.powsybl.caseserver.CaseException;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.parsers.FileNameInfos;
import com.powsybl.caseserver.parsers.FileNameParser;
import com.powsybl.caseserver.parsers.FileNameParsers;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.Network;
import org.springframework.web.multipart.MultipartFile;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

    default void createCaseMetadataEntity(UUID newCaseUuid, boolean withExpiration, CaseMetadataRepository caseMetadataRepository) {
        LocalDateTime expirationTime = null;
        if (withExpiration) {
            expirationTime = LocalDateTime.now(ZoneOffset.UTC).plusHours(1);
        }
        caseMetadataRepository.save(new CaseMetadataEntity(newCaseUuid, expirationTime));
    }

    default Importer getImporterOrThrowsException(Path caseFile, ComputationManager computationManager) {
        DataSource dataSource = DataSource.fromPath(caseFile);
        Importer importer = Importer.find(dataSource, computationManager);
        if (importer == null) {
            throw CaseException.createFileNotImportable(caseFile);
        }
        return importer;
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
}
