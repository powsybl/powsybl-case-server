/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.CaseException;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.computation.ComputationManager;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.Network;
import jakarta.annotation.PostConstruct;
import org.apache.commons.compress.utils.FileNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static com.powsybl.caseserver.CaseException.createDirectoryNotFound;
import static com.powsybl.caseserver.Utils.*;
import static com.powsybl.caseserver.service.S3CaseService.DELIMITER;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
public class FsCaseService implements CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FsCaseService.class);

    private FileSystem fileSystem = FileSystems.getDefault();

    private ComputationManager computationManager = LocalComputationManager.getDefault();

    private final CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private NotificationService notificationService;

    @Autowired
    private CaseInfosService caseInfosService;

    @Value("${case-home:#{systemProperties['user.home']}}")
    private String caseHome;

    @Value("${case-subpath}")
    private String caseSubpath;

    private String rootDirectory;

    public FsCaseService(CaseMetadataRepository caseMetadataRepository) {
        this.caseMetadataRepository = caseMetadataRepository;
    }

    @PostConstruct
    private void postConstruct() {
        rootDirectory = caseHome + DELIMITER + caseSubpath;
    }

    @Override
    public String getRootDirectory() {
        return rootDirectory;
    }

    @Override
    public String getFormat(UUID caseUuid) {
        Path file = getCaseFile(caseUuid);
        return getFormat(file);
    }

    String getFormat(Path caseFile) {
        Importer importer = getImporterOrThrowsException(caseFile);
        return importer.getFormat();
    }

    @Override
    public List<CaseInfos> getCases() {
        try (Stream<Path> walk = Files.walk(getStorageRootDir())) {
            return walk.filter(Files::isRegularFile)
                    .map(this::getCaseInfos)
                    .filter(Objects::nonNull)
                    .map(this::convertCaseInfosForPlainFile)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private CaseInfos getCaseInfos(Path file) {
        Objects.requireNonNull(file);
        try {
            return createInfos(file, UUID.fromString(file.getParent().getFileName().toString()));
        } catch (Exception e) {
            LOGGER.error("Error processing file {}: {}", file.getFileName(), e.getMessage(), e);
            return null;
        }
    }

    @Override
    public String getCaseName(UUID caseUuid) {
        Path file = getCaseFile(caseUuid);
        if (file == null) {
            throw createDirectoryNotFound(caseUuid);
        }
        CaseInfos caseInfos = getCaseInfos(file);
        if (caseInfos == null) {
            throw CaseException.createFileNameNotFound(caseUuid);
        }
        if (Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
            return removeExtension(caseInfos.getName(), GZIP_EXTENSION);
        }
        return caseInfos.getName();
    }

    @Override
    public CaseInfos getCaseInfos(UUID caseUuid) {
        Path file = getCaseFile(caseUuid);
        if (file == null) {
            LOGGER.error("The directory with the following uuid doesn't exist: {}", caseUuid);
            return null;
        }
        CaseInfos caseInfos = getCaseInfos(file);
        return this.convertCaseInfosForPlainFile(caseInfos);
    }

    public Path getCaseFile(UUID caseUuid) {
        return walkCaseDirectory(getStorageRootDir().resolve(caseUuid.toString()));
    }

    Path getCaseDirectory(UUID caseUuid) {
        Path caseDirectory = getStorageRootDir().resolve(caseUuid.toString());
        if (Files.exists(caseDirectory) && Files.isDirectory(caseDirectory)) {
            return caseDirectory;
        }
        throw CaseException.createDirectoryNotFound(caseUuid);
    }

    public Path walkCaseDirectory(Path caseDirectory) {
        if (Files.exists(caseDirectory) && Files.isDirectory(caseDirectory)) {
            try (Stream<Path> pathStream = Files.walk(caseDirectory)) {
                Optional<Path> pathOpt = pathStream.filter(file -> !Files.isDirectory(file)).findFirst();
                if (pathOpt.isEmpty()) {
                    throw CaseException.createDirectoryEmpty(caseDirectory);
                }
                return pathOpt.get();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return null;
    }

    @Override
    public boolean caseExists(UUID caseName) {
        checkStorageInitialization();
        Path caseFile = getCaseFile(caseName);
        if (caseFile == null) {
            return false;
        }
        return Files.exists(caseFile) && Files.isRegularFile(caseFile);
    }

    @Override
    public UUID importCase(MultipartFile mpf, boolean withExpiration, boolean withIndexation) {
        checkStorageInitialization();

        UUID caseUuid = UUID.randomUUID();
        Path uuidDirectory = getStorageRootDir().resolve(caseUuid.toString());

        String caseName = Objects.requireNonNull(mpf.getOriginalFilename()).trim();
        validateCaseName(caseName);
        String compressionFormat = FileNameUtils.getExtension(Paths.get(caseName));

        if (Files.exists(uuidDirectory)) {
            throw CaseException.createDirectoryAreadyExists(uuidDirectory.toString());
        }

        Path caseFile;
        try {
            byte[] fileBytes = mpf.getBytes();
            String fileNamePath = caseName;
            if (!isCompressedCaseFile(caseName) && !isArchivedCaseFile(caseName)) {
                // not compressed files only
                fileNamePath += GZIP_EXTENSION;
                fileBytes = compress(fileBytes);
            }
            Files.createDirectory(uuidDirectory);
            caseFile = uuidDirectory.resolve(fileNamePath);
            Files.write(caseFile, fileBytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Importer importer;
        try {
            importer = getImporterOrThrowsException(caseFile);
        } catch (CaseException e) {
            try {
                Files.deleteIfExists(caseFile);
                Files.deleteIfExists(uuidDirectory);
            } catch (IOException e2) {
                LOGGER.error(e2.toString(), e2);
            }
            throw e;
        }

        String format = importer.getFormat();
        createCaseMetadataEntity(caseUuid, withExpiration, withIndexation, caseName, compressionFormat, format);
        String caseInfoFileName = caseFile.getFileName().toString();
        if (Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
            caseInfoFileName = removeExtension(caseInfoFileName, GZIP_EXTENSION);
        }
        CaseInfos caseInfos = createInfos(caseInfoFileName, caseUuid, format);
        if (withIndexation) {
            caseInfosService.addCaseInfos(caseInfos);
        }
        notificationService.sendImportMessage(caseInfos.createMessage());
        return caseUuid;
    }

    @Override
    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {
        try {
            Path existingCaseFile = getCaseFile(sourceCaseUuid);
            if (existingCaseFile == null || existingCaseFile.getParent() == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + " not found");
            }

            UUID newCaseUuid = UUID.randomUUID();
            Path newCaseUuidDirectory = existingCaseFile.getParent().getParent().resolve(newCaseUuid.toString());
            Path newCaseFile;
            Files.createDirectory(newCaseUuidDirectory);
            newCaseFile = newCaseUuidDirectory.resolve(existingCaseFile.getFileName());
            Files.copy(existingCaseFile, newCaseFile, StandardCopyOption.COPY_ATTRIBUTES);

            CaseMetadataEntity existingCase = getCaseMetaDataEntity(sourceCaseUuid);
            CaseInfos caseInfos = createInfos(existingCase.getOriginalFilename(), newCaseUuid, existingCase.getFormat());
            if (existingCase.isIndexed()) {
                caseInfosService.addCaseInfos(caseInfos);
            }
            createCaseMetadataEntity(newCaseUuid, withExpiration, existingCase.isIndexed());
            notificationService.sendImportMessage(caseInfos.createMessage());
            return newCaseUuid;

        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "An error occurred during case duplication");
        }
    }

    private CaseInfos createInfos(Path caseFile, UUID caseUuid) {
        return createInfos(caseFile.getFileName().toString(), caseUuid, getFormat(caseFile));
    }

    @Override
    public Optional<Network> loadNetwork(UUID caseUuid) {
        checkStorageInitialization();

        Path caseFile = getCaseFile(caseUuid);
        if (caseFile == null) {
            return Optional.empty();
        }

        if (Files.exists(caseFile) && Files.isRegularFile(caseFile)) {
            Network network = Network.read(caseFile);
            if (network == null) {
                throw CaseException.createFileNotImportable(caseFile);
            }
            return Optional.of(network);
        }
        return Optional.empty();
    }

    void deleteDirectoryRecursively(Path caseDirectory) {
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(caseDirectory)) {
            paths.forEach(file -> {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            Files.delete(caseDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void deleteCase(UUID caseUuid) {
        checkStorageInitialization();
        Path caseDirectory = getCaseDirectory(caseUuid);
        deleteDirectoryRecursively(caseDirectory);
        caseInfosService.deleteCaseInfosByUuid(caseUuid.toString());
        caseMetadataRepository.deleteById(caseUuid);
    }

    @Override
    public void deleteAllCases() {
        checkStorageInitialization();

        Path rootDirectoryPath = getStorageRootDir();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(rootDirectoryPath)) {
            paths.forEach(this::deleteDirectoryRecursively);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        caseInfosService.deleteAllCaseInfos();
        caseMetadataRepository.deleteAll();
    }

    public Path getStorageRootDir() {
        return fileSystem.getPath(getRootDirectory());
    }

    private boolean isStorageCreated() {
        Path storageRootDir = getStorageRootDir();
        return Files.exists(storageRootDir) && Files.isDirectory(storageRootDir);
    }

    public void checkStorageInitialization() {
        if (!isStorageCreated()) {
            throw CaseException.createStorageNotInitialized(getStorageRootDir());
        }
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem);
    }

    @Override
    public void setComputationManager(ComputationManager computationManager) {
        this.computationManager = Objects.requireNonNull(computationManager);
    }

    @Override
    public ComputationManager getComputationManager() {
        return computationManager;
    }

    @Override
    public Optional<byte[]> getCaseBytes(UUID caseUuid) {
        checkStorageInitialization();

        Path caseFile = getCaseFile(caseUuid);
        if (caseFile == null) {
            return Optional.empty();
        }
        if (Files.exists(caseFile) && Files.isRegularFile(caseFile)) {
            try {
                byte[] bytes = Files.readAllBytes(caseFile);
                if (Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
                    bytes = decompress(bytes);
                }
                return Optional.of(bytes);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return Optional.empty();
    }

    @Override
    public CaseMetadataRepository getCaseMetadataRepository() {
        return caseMetadataRepository;
    }

    private CaseInfos convertCaseInfosForPlainFile(CaseInfos caseInfos) {
        if (caseInfos != null && Boolean.TRUE.equals(isUploadedAsPlainFile(caseInfos.getUuid()))) {
            return createInfos(removeExtension(caseInfos.getName(), GZIP_EXTENSION), caseInfos.getUuid(), caseInfos.getFormat());
        }
        return caseInfos;
    }

}
