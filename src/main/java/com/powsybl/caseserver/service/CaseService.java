/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

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
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static com.powsybl.caseserver.Utils.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
public interface CaseService {
    enum StorageType { FS, S3 }

    default void validateCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        if (!caseName.matches("[^<>:\"/|?*]+(\\.[\\w]+)")) {
            throw CaseException.createIllegalCaseName(caseName);
        }
    }

    default CaseMetadataEntity getCaseMetaDataEntity(UUID caseUuid) {
        return getCaseMetadataRepository().findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Metadata of case " + caseUuid + NOT_FOUND));
    }

    default Boolean isUploadedAsPlainFile(UUID caseUuid) {
        String name = getCaseMetaDataEntity(caseUuid).getOriginalFilename();
        return name != null && !isCompressedCaseFile(name) && !isArchivedCaseFile(name);
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

    default void createCaseMetadataEntity(UUID newCaseUuid, boolean withExpiration, boolean withIndexation, String originalFilename, String compressionFormat, String format) {
        Instant expirationTime = null;
        if (withExpiration) {
            expirationTime = Instant.now().plus(1, ChronoUnit.HOURS);
        }
        getCaseMetadataRepository().save(new CaseMetadataEntity(newCaseUuid, expirationTime, withIndexation, originalFilename, compressionFormat, format));
    }

    default List<CaseInfos> getMetadata(List<UUID> ids) {
        List<CaseInfos> cases = new ArrayList<>();
        ids.forEach(caseUuid -> {
            CaseInfos caseInfos = getCaseInfos(caseUuid);
            if (Objects.nonNull(caseInfos)) {
                cases.add(caseInfos);
            }
        });
        return cases;
    }

    default Importer getImporterOrThrowsException(Path caseFile) {
        DataSource dataSource = DataSource.fromPath(caseFile);
        Importer importer = Importer.find(dataSource, getComputationManager());
        if (importer == null) {
            throw CaseException.createFileNotImportable(caseFile);
        }
        return importer;
    }

    default List<CaseInfos> getCasesToReindex() {
        Set<UUID> casesToReindex = getCaseMetadataRepository().findAllByIndexedTrue()
                .stream()
                .map(CaseMetadataEntity::getId)
                .collect(Collectors.toSet());
        return getCases().stream().filter(c -> casesToReindex.contains(c.getUuid())).toList();
    }

    StorageType getStorageType();

    List<CaseInfos> getCases();

    boolean caseExists(UUID caseUuid);

    CaseInfos getCaseInfos(UUID caseUuid);

    String getFormat(UUID caseUuid);

    String getCaseName(UUID caseUuid);

    Optional<InputStream> getCaseStream(UUID caseUuid);

    UUID importCase(MultipartFile file, boolean withExpiration, boolean withIndexation, UUID caseUuid);

    UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration);

    void deleteCase(UUID caseUuid);

    void deleteAllCases();

    void setComputationManager(ComputationManager computationManager);

    CaseMetadataRepository getCaseMetadataRepository();

    ComputationManager getComputationManager();

    String getRootDirectory();
}
