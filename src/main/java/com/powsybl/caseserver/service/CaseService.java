/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.CaseException;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.dto.ExportCaseInfos;
import com.powsybl.caseserver.parsers.FileNameInfos;
import com.powsybl.caseserver.parsers.FileNameParser;
import com.powsybl.caseserver.parsers.FileNameParsers;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.commons.datasource.DataSourceUtil;
import com.powsybl.commons.datasource.MemDataSource;
import com.powsybl.computation.ComputationManager;
import com.powsybl.iidm.network.Exporter;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.Network;
import org.springframework.http.HttpStatus;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.powsybl.caseserver.Utils.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
public interface CaseService {
    default void validateCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        if (!caseName.matches("[^<>:\"/|?*]+(\\.[\\w]+)")) {
            throw CaseException.createIllegalCaseName(caseName);
        }
    }

    default CaseMetadataEntity getCaseMetaDataEntity(UUID caseUuid) {
        return getCaseMetadataRepository().findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "case " + caseUuid + NOT_FOUND));
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

    default void createCaseMetadataEntity(UUID newCaseUuid, boolean withExpiration, boolean withIndexation) {
        createCaseMetadataEntity(newCaseUuid, withExpiration, withIndexation, null, null, null);
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

    default byte[] createZipFile(Collection<String> names, MemDataSource dataSource) throws IOException {
        try (var outputStream = new ByteArrayOutputStream();
             var zipOutputStream = new ZipOutputStream(outputStream)) {
            for (String name : names) {
                zipOutputStream.putNextEntry(new ZipEntry(name));
                zipOutputStream.write(dataSource.getData(name));
                zipOutputStream.closeEntry();
            }
            return outputStream.toByteArray();
        }
    }

    default Optional<ExportCaseInfos> exportCase(UUID caseUuid, String format, String fileName, Map<String, Object> formatParameters) throws IOException {
        if (!Exporter.getFormats().contains(format)) {
            throw CaseException.createUnsupportedFormat(format);
        }

        var optionalNetwork = loadNetwork(caseUuid);
        if (optionalNetwork.isPresent()) {
            var network = optionalNetwork.get();
            var memDataSource = new MemDataSource();
            Properties exportProperties = null;
            if (formatParameters != null) {
                exportProperties = new Properties();
                exportProperties.putAll(formatParameters);
            }

            network.write(format, exportProperties, memDataSource);

            var listNames = memDataSource.listNames(".*");
            String fileOrNetworkName = fileName != null ? fileName : DataSourceUtil.getBaseName(getCaseName(caseUuid));
            byte[] networkData;
            if (listNames.size() == 1) {
                String extension = listNames.iterator().next();
                fileOrNetworkName += extension;
                networkData = memDataSource.getData(extension);
            } else {
                fileOrNetworkName += ".zip";
                networkData = createZipFile(listNames, memDataSource);
            }
            return Optional.of(new ExportCaseInfos(fileOrNetworkName, networkData));
        } else {
            return Optional.empty();
        }
    }

    default List<CaseInfos> getCasesToReindex() {
        Set<UUID> casesToReindex = getCaseMetadataRepository().findAllByIndexedTrue()
                .stream()
                .map(CaseMetadataEntity::getId)
                .collect(Collectors.toSet());
        return getCases().stream().filter(c -> casesToReindex.contains(c.getUuid())).toList();
    }

    List<CaseInfos> getCases();

    boolean caseExists(UUID caseUuid);

    CaseInfos getCaseInfos(UUID caseUuid);

    String getFormat(UUID caseUuid);

    String getCaseName(UUID caseUuid);

    Optional<Network> loadNetwork(UUID caseUuid);

    Optional<byte[]> getCaseBytes(UUID caseUuid);

    UUID importCase(MultipartFile file, boolean withExpiration, boolean withIndexation);

    UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration);

    void deleteCase(UUID caseUuid);

    void deleteAllCases();

    void setComputationManager(ComputationManager computationManager);

    CaseMetadataRepository getCaseMetadataRepository();

    ComputationManager getComputationManager();

    String getRootDirectory();
}
