/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.server;

import com.powsybl.caseserver.CaseException;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.computation.ComputationManager;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.Network;
import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */

@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
public class S3CaseService implements CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3CaseService.class);

    private ComputationManager computationManager = LocalComputationManager.getDefault();

    private final CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private CaseInfosService caseInfosService;

    @Autowired
    NotificationService notificationService;

    @Value("${spring.cloud.aws.bucket}")
    private String bucketName;

    private static final String CASES_PREFIX = "gsi-cases/";

    public static final String NOT_FOUND = " not found";

    @Autowired
    private S3Client s3Client;

    public S3CaseService(CaseMetadataRepository caseMetadataRepository) {
        this.caseMetadataRepository = caseMetadataRepository;
    }

    String getFormat(Path caseFile) {
        Importer importer = getImporterOrThrowsException(caseFile, computationManager);
        return importer.getFormat();
    }

    // creates a directory, and then in this directory, initializes a file with content.
    // After applying f to the file, deletes the file and the directory.
    private <R, T1 extends Throwable, T2 extends Throwable> R withTempCopy(UUID caseUuid, String filename,
                                                                           FailableConsumer<Path, T1> contentInitializer, FailableFunction<Path, R, T2> f) {
        Path tempdirPath;
        Path tempCasePath;
        try {
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
            tempdirPath = Files.createTempDirectory(caseUuid.toString(), attr);
            // after this line, need to cleanup the dir
        } catch (IOException e) {
            throw CaseException.createTempDirectory(caseUuid, e);
        }
        try {
            tempCasePath = tempdirPath.resolve(filename);
            try {
                contentInitializer.accept(tempCasePath);
            } catch (CaseException e) {
                throw CaseException.initTempFile(caseUuid, e);
            } catch (Throwable ex) {
                throw CaseException.initTempFile(caseUuid);
            }
            // after this line, need to cleanup the file
            try {
                try {
                    return f.apply(tempCasePath);
                } catch (CaseException e) {
                    throw CaseException.createFileNotImportable(tempdirPath);
                } catch (Throwable t) {
                    throw CaseException.processTempFile(caseUuid);
                }
            } finally {
                try {
                    Files.delete(tempCasePath);
                } catch (IOException e) {
                    LOGGER.error("Error cleaning up temporary case file", e);
                }
            }
        } finally {
            try {
                Files.delete(tempdirPath);
            } catch (IOException e) {
                LOGGER.error("Error cleaning up temporary case dir", e);
            }
        }
    }

    // downloads from s3 and cleanup
    public <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, FailableFunction<Path, R, T> f) {
        return withS3DownloadedTempPath(caseUuid, null, f);
    }

    public <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, String caseFileKeyFromCaller, FailableFunction<Path, R, T> f) {
        String caseFileKey = Objects.requireNonNullElse(caseFileKeyFromCaller, getCaseFileObjectKey(caseUuid));
        String filename = parseFilenameFromKey(caseFileKey);
        return withTempCopy(caseUuid, filename, path ->
                        s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(caseFileKey).build(), path),
                f);
    }

    @Override
    public String getFormat(UUID caseUuid) {
        final var caseInfos = caseInfosService.getCaseInfosByUuid(caseUuid.toString());
        return caseInfos.map(CaseInfos::getFormat).orElse(null);
    }

    // key format is "gsi-cases/UUID/filename"
    private UUID parseUuidFromKey(String key) {
        int firstSlash = key.indexOf('/');
        int secondSlash = key.indexOf('/', firstSlash + 1);
        return UUID.fromString(key.substring(firstSlash + 1, secondSlash));
    }

    private String parseFilenameFromKey(String key) {
        int firstSlash = key.indexOf('/');
        int secondSlash = key.indexOf('/', firstSlash + 1);
        return key.substring(secondSlash + 1);
    }

    private String uuidToPrefixKey(UUID uuid) {
        return CASES_PREFIX + uuid.toString() + "/";
    }

    private String uuidAndFilenameToKey(UUID uuid, String filename) {
        return uuidToPrefixKey(uuid) + filename;
    }

    private List<S3Object> getCasesSummaries(String prefix) {

        List<S3Object> s3Objects = new ArrayList<>();

        ListObjectsV2Iterable listObjectsV2Iterable = s3Client.listObjectsV2Paginator(getListObjectsV2Request(prefix));
        listObjectsV2Iterable.iterator().forEachRemaining(listObjectsChunk ->
            s3Objects.addAll(listObjectsChunk.contents())
        );

        return s3Objects;

    }

    private ListObjectsV2Request getListObjectsV2Request(String prefix) {
        return ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
    }

    private List<S3Object> getCaseFileSummaries(UUID caseUuid) {
        List<S3Object> files = getCasesSummaries(uuidToPrefixKey(caseUuid));
        if (files.size() > 1) {
            LOGGER.warn("Multiple files for case {}", caseUuid);
        }
        return files;
    }

    public String getCaseFileObjectKey(UUID caseUuid) {
        List<String> allKeys = getCaseFileSummaries(caseUuid).stream()
                .map(S3Object::key)
                .toList();
        return allKeys.stream()
                .filter(key -> key.contains(".zip"))
                .findFirst()
                .orElse(allKeys.isEmpty() ? null : allKeys.get(0));
    }

    private CaseInfos infosFromDownloadCaseFileSummary(S3Object objectSummary) {
        UUID uuid = parseUuidFromKey(objectSummary.key());
        Optional<CaseInfos> caseInfos = caseInfosService.getCaseInfosByUuid(uuid.toString());
        return caseInfos.orElse(null);
    }

    private List<CaseInfos> infosFromDownloadCaseFileSummaries(List<S3Object> objectSummaries) {
        List<CaseInfos> caseInfosList = new ArrayList<>();
        for (S3Object objectSummary : objectSummaries) {
            final var caseInfo = infosFromDownloadCaseFileSummary(objectSummary);
            if (Objects.nonNull(caseInfo)) {
                caseInfosList.add(caseInfo);
            }
        }
        return caseInfosList;
    }

    @Override
    public CaseInfos getCaseInfos(UUID caseUuid) {
        var caseFileSummaries = getCaseFileSummaries(caseUuid);
        if (caseFileSummaries.isEmpty()) {
            return null;
        } else {
            return infosFromDownloadCaseFileSummary(caseFileSummaries.get(0));
        }
    }

    @Override
    public String getCaseName(UUID caseUuid) {
        CaseInfos caseInfos = getCaseInfos(caseUuid);
        return caseInfos.getName();
    }

    @Override
    public Optional<byte[]> getCaseBytes(UUID caseUuid) {
        String caseFileKey = getCaseFileObjectKey(caseUuid);

        if (Objects.nonNull(caseFileKey)) {
            try {
                GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                        .bucket(bucketName)
                        .key(caseFileKey)
                        .build();

                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
                byte[] content = objectBytes.asByteArray();
                return Optional.of(content);
            } catch (NoSuchKeyException e) {
                LOGGER.error("The expected key does not exist in the bucket s3 : {}", caseFileKey);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<CaseInfos> getCases() {
        List<S3Object> s3ObjectSummaries = getCasesSummaries(CASES_PREFIX);
        return infosFromDownloadCaseFileSummaries(s3ObjectSummaries);
    }

    @Override
    public boolean caseExists(UUID uuid) {
        return !getCasesSummaries(uuidToPrefixKey(uuid)).isEmpty();
    }

    @Override
    public UUID importCase(MultipartFile mpf, boolean withExpiration) {
        UUID caseUuid = UUID.randomUUID();

        String caseName = mpf.getOriginalFilename();
        validateCaseName(caseName);

        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);

        try (InputStream inputStream = mpf.getInputStream()) {
            String key = uuidAndFilenameToKey(caseUuid, caseName);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(mpf.getContentType())
                    .build();

            // Use putObject to upload the file
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, mpf.getSize()));
            if (mpf.getContentType().equals("application/zip")) {
                importZipContent(mpf.getInputStream(), caseUuid);
            }
        } catch (IOException e) {
            throw CaseException.createFileNotImportable(caseName);
        }

        createCaseMetadataEntity(caseUuid, withExpiration, caseMetadataRepository);
        CaseInfos caseInfos = createInfos(caseName, caseUuid, format);
        caseInfosService.addCaseInfos(caseInfos);
        notificationService.sendImportMessage(caseInfos.createMessage());

        return caseUuid;
    }

    private void importZipContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (ZipInputStream zipInputStream = new ZipInputStream(inputStream)) {
            ZipEntry entry;

            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    processEntry(caseUuid, zipInputStream, entry);
                }
                zipInputStream.closeEntry();
            }
        }
    }

    private void processEntry(UUID caseUuid, ZipInputStream zipInputStream, ZipEntry entry) throws IOException {
        String fileName = findFileName(entry.getName());
        String extractedKey = uuidAndFilenameToKey(caseUuid, fileName);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = zipInputStream.read(buffer)) > 0) {
            byteArrayOutputStream.write(buffer, 0, length);
        }
        byte[] fileBytes = byteArrayOutputStream.toByteArray();

        PutObjectRequest extractedFileRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(extractedKey)
                .contentType(Files.probeContentType(Paths.get(fileName))) // Detect the MIME type
                .build();

        s3Client.putObject(extractedFileRequest, RequestBody.fromBytes(fileBytes));
    }

    private String findFileName(String path) {
        String[] parts = path.split("/");
        return parts[parts.length - 1];
    }

    @Override
    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {

        String sourceKey = getCaseFileObjectKey(sourceCaseUuid);
        CaseInfos existingCaseInfos = caseInfosService.getCaseInfosByUuid(sourceCaseUuid.toString())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + NOT_FOUND));
        UUID newCaseUuid = UUID.randomUUID();
        String targetKey = uuidAndFilenameToKey(newCaseUuid, parseFilenameFromKey(sourceKey));
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(sourceKey)
                .destinationBucket(bucketName)
                .destinationKey(targetKey)
                .build();
        try {
            s3Client.copyObject(copyObjectRequest);
        } catch (S3Exception e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "source case " + sourceCaseUuid + NOT_FOUND);
        }
        Optional<byte[]> caseBytes = getCaseBytes(newCaseUuid);
        if (caseBytes.isPresent()) {
            try {
                importZipContent(new ByteArrayInputStream(caseBytes.get()), newCaseUuid);
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            }

        }
        CaseInfos caseInfos = createInfos(existingCaseInfos.getName(), newCaseUuid, existingCaseInfos.getFormat());
        caseInfosService.addCaseInfos(caseInfos);
        createCaseMetadataEntity(newCaseUuid, withExpiration, caseMetadataRepository);
        notificationService.sendImportMessage(caseInfos.createMessage());
        return newCaseUuid;
    }

    @Transactional
    @Override
    public void disableCaseExpiration(UUID caseUuid) {
        CaseMetadataEntity caseMetadataEntity = caseMetadataRepository.findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "case " + caseUuid + NOT_FOUND));
        caseMetadataEntity.setExpirationDate(null);
    }

    @Override
    public Optional<Network> loadNetwork(UUID caseUuid) {

        if (!caseExists(caseUuid)) {
            return Optional.empty();
        }

        return Optional.of(withS3DownloadedTempPath(caseUuid, path -> {
            Network network = Network.read(path);
            if (network == null) {
                throw CaseException.createFileNotImportable(path);
            }
            return network;
        }));
    }

    @Override
    public void deleteCase(UUID caseUuid) {
        String prefixKey = uuidToPrefixKey(caseUuid);
        List<ObjectIdentifier> objectsToDelete = s3Client.listObjectsV2(builder -> builder.bucket(bucketName).prefix(prefixKey))
            .contents()
            .stream()
            .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
            .toList();

        if (!objectsToDelete.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(Delete.builder().objects(objectsToDelete).build())
                .build();
            s3Client.deleteObjects(deleteObjectsRequest);
            caseInfosService.deleteCaseInfosByUuid(caseUuid.toString());
            caseMetadataRepository.deleteById(caseUuid);
        }
    }

    @Override
    public void deleteAllCases() {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(CASES_PREFIX)
                .build();

        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
        List<ObjectIdentifier> objectsToDelete = listObjectsResponse.contents().stream()
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .toList();

        if (!objectsToDelete.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder().objects(objectsToDelete).build())
                    .build();

            s3Client.deleteObjects(deleteObjectsRequest);
        }

        caseInfosService.deleteAllCaseInfos();
        caseMetadataRepository.deleteAll();

    }

    @Override
    public void setComputationManager(ComputationManager computationManager) {
        this.computationManager = Objects.requireNonNull(computationManager);
    }

    public List<CaseInfos> searchCases(String query) {
        return caseInfosService.searchCaseInfos(query);
    }

    @Override
    public void reindexAllCases() {
        caseInfosService.recreateAllCaseInfos(getCases());
    }

    @Override
    public List<CaseInfos> getMetadata(List<UUID> ids) {
        List<CaseInfos> cases = new ArrayList<>();
        ids.forEach(caseUuid -> {
            CaseInfos caseInfos = getCaseInfos(caseUuid);
            if (Objects.nonNull(caseInfos)) {
                cases.add(caseInfos);
            }
        });
        return cases;
    }
}
