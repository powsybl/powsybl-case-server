/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
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
import com.powsybl.ws.commons.SecuredZipInputStream;
import org.apache.commons.io.FileUtils;
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
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.powsybl.caseserver.CaseException.createDirectoryNotFound;
import static com.powsybl.caseserver.dto.CaseInfos.FORMAT_HEADER_KEY;

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
            if (Paths.get(filename).getParent() != null) {
                Files.createDirectory(Paths.get(tempdirPath.toString(), Paths.get(filename).getParent().toString()), attr);
            }
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
                FileUtils.deleteDirectory(tempdirPath.toFile());
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
        String caseFileKey = getCaseFileObjectKey(caseUuid);
        HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(caseFileKey)
                .build());

        String format = headObjectResponse.metadata().get(FORMAT_HEADER_KEY);
        if (format == null) {
            return withS3DownloadedTempPath(caseUuid, this::getFormat);

        }
        return format;
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
        return getCaseInfos(uuid);
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
        return new CaseInfos(caseUuid, getCaseName(caseUuid), getFormat(caseUuid));
    }

    @Override
    public String getCaseName(UUID caseUuid) {
        List<S3Object> files = getCasesSummaries(uuidToPrefixKey(caseUuid));
        if (files.isEmpty()) {
            throw createDirectoryNotFound(caseUuid);
        }
        if (files.size() > 1) {
            String fileName;
            for (S3Object file : files) {
                fileName = Paths.get(file.key()).getFileName().toString();
                if (fileName.matches(".*\\.zip")) {
                    return fileName;
                }
            }
            throw CaseException.createOriginalFileNotFound(caseUuid);
        } else {
            return Paths.get(files.get(0).key()).getFileName().toString();
        }
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

    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        if (getCaseFileSummaries(caseUuid).size() > 1 && fileName.equals(getCaseName(caseUuid))) {
            return Boolean.FALSE;
        }
        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(uuidToPrefixKey(caseUuid) + fileName)
                .build();
        try {
            s3Client.headObject(headObjectRequest);
            return Boolean.TRUE;
        } catch (NoSuchKeyException e) {
            return Boolean.FALSE;
        }
    }

    public Set<String> listName(UUID caseUuid, String regex) {
        List<S3Object> s3Objects = getCaseFileSummaries(caseUuid);
        List<String> names = s3Objects.stream().map(obj -> Paths.get(obj.key()).toString().replace(CASES_PREFIX + caseUuid.toString() + "/", "")).collect(Collectors.toList());
        if (names.size() > 1) {
            names = names.stream().filter(name -> !name.equals(getCaseName(caseUuid))).collect(Collectors.toList());
        }
        return names.stream().filter(name -> name.matches(regex)).collect(Collectors.toSet());
    }

    @Override
    public UUID importCase(MultipartFile mpf, boolean withExpiration, boolean withIndexation) {
        UUID caseUuid = UUID.randomUUID();

        String caseName = mpf.getOriginalFilename();
        validateCaseName(caseName);

        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);

        try (InputStream inputStream = mpf.getInputStream()) {
            String key = uuidAndFilenameToKey(caseUuid, caseName);
            Map<String, String> caseMetadata = new HashMap<>();
            caseMetadata.put(FORMAT_HEADER_KEY, format);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .metadata(caseMetadata)
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

        createCaseMetadataEntity(caseUuid, withExpiration, withIndexation, caseMetadataRepository);
        CaseInfos caseInfos = createInfos(caseName, caseUuid, format);
        if (withIndexation) {
            caseInfosService.addCaseInfos(caseInfos);
        }
        notificationService.sendImportMessage(caseInfos.createMessage());

        return caseUuid;
    }

    private void importZipContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (ZipInputStream zipInputStream = new SecuredZipInputStream(inputStream, 1000, 500000000)) {
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
        String fileName = entry.getName();
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

    @Override
    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {
        if (!caseExists(sourceCaseUuid)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + NOT_FOUND);
        }

        String sourceKey = getCaseFileObjectKey(sourceCaseUuid);
        CaseInfos existingCaseInfos = getCaseInfos(sourceCaseUuid);
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
        CaseMetadataEntity existingCase = getCaseMetaDataEntity(sourceCaseUuid);

        CaseInfos caseInfos = createInfos(existingCaseInfos.getName(), newCaseUuid, existingCaseInfos.getFormat());
        if (existingCase.isIndexed()) {
            caseInfosService.addCaseInfos(caseInfos);
        }
        createCaseMetadataEntity(newCaseUuid, withExpiration, existingCase.isIndexed(), caseMetadataRepository);
        notificationService.sendImportMessage(caseInfos.createMessage());
        return newCaseUuid;
    }

    // TODO should not be duplicated with FSCaseService
    public List<CaseInfos> getCasesToReindex() {
        Set<UUID> casesToReindex = caseMetadataRepository.findAllByIndexedTrue()
                .stream()
                .map(CaseMetadataEntity::getId)
                .collect(Collectors.toSet());
        return getCases().stream().filter(c -> casesToReindex.contains(c.getUuid())).toList();
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

    private CaseMetadataEntity getCaseMetaDataEntity(UUID caseUuid) {
        return caseMetadataRepository.findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "case " + caseUuid + " not found"));
    }

}
