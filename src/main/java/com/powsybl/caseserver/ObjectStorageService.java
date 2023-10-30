/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.computation.ComputationManager;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.iidm.network.Importer;
import com.powsybl.iidm.network.Network;

import org.apache.commons.lang3.Functions.FailableConsumer;
import org.apache.commons.lang3.Functions.FailableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */

@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
public class ObjectStorageService implements CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageService.class);

    private static final String CATEGORY_BROKER_OUTPUT = ObjectStorageService.class.getName() + ".output-broker-messages";

    private static final Logger OUTPUT_MESSAGE_LOGGER = LoggerFactory.getLogger(CATEGORY_BROKER_OUTPUT);

    private ComputationManager computationManager = LocalComputationManager.getDefault();

    private CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private StreamBridge caseInfosPublisher;

    @Autowired
    private CaseInfosService caseInfosService;

    @Value("${spring.cloud.aws.bucket}")
    private String bucketName;

    private static final String CASES_PREFIX = "gsi-cases/";

    @Autowired
    private S3Client s3Client;

    public ObjectStorageService(CaseMetadataRepository caseMetadataRepository) {
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
        Path tempdir;
        Path tempCasePath;
        try {
            tempdir = Files.createTempDirectory(caseUuid.toString());
            // after this line, need to cleanup the dir
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            tempCasePath = tempdir.resolve(filename);
            try {
                contentInitializer.accept(tempCasePath);
            } catch (CaseException e) {
                throw e; // don't wrap our exceptions
            } catch (Throwable ex) {
                throw new RuntimeException(ex);
            }
            // after this line, need to cleanup the file
            try {
                try {
                    return f.apply(tempCasePath);
                } catch (CaseException e) {
                    throw e; // don't wrap our exceptions
                } catch (Throwable t) {
                    throw new RuntimeException(t);
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
                Files.delete(tempdir);
            } catch (IOException e) {
                LOGGER.error("Error cleaning up temporary case dir", e);
            }
        }
    }

    // downloads from s3 and cleanup
    public <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, FailableFunction<Path, R, T> f) {
        String caseFileKey = getCaseFileObjectKey(caseUuid);
        String filename = parseFilenameFromKey(caseFileKey);
        return withTempCopy(caseUuid, filename, path ->
                        s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(caseFileKey).build(), path.toFile().toPath()),
                f);
    }

    public String getFormat(UUID caseUuid) {

        String caseFileKey = getCaseFileObjectKey(caseUuid);

        HeadObjectResponse headObjectResponse = s3Client.headObject(HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(caseFileKey)
                .build());

        Map<String, String> userMetadata = headObjectResponse.metadata();

        String format = userMetadata.get("format");

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

    //TODO try to remove amazon client objects from APIs
    private List<S3Object> getCasesSummaries(String prefix) {

        List<S3Object> s3Objects = new ArrayList<>();

        ListObjectsV2Iterable listObjectsV2Iterable = s3Client.listObjectsV2Paginator(getListObjectsV2Request());
        listObjectsV2Iterable.iterator().forEachRemaining(listObjectsChunk -> {
            s3Objects.addAll(listObjectsChunk.contents());

        });

        return s3Objects.stream()
                .filter(x -> x.key().contains(prefix)).collect(Collectors.toList());
    }

    private ListObjectsV2Request getListObjectsV2Request() {
        return ListObjectsV2Request.builder().bucket(bucketName).prefix(CASES_PREFIX).build();
    }

    private List<S3Object> getCaseFileSummaries(UUID caseUuid) {
        List<S3Object> files = getCasesSummaries(uuidToPrefixKey(caseUuid));
        if (files.size() > 1) {
            LOGGER.warn("Multiple files for case " + caseUuid);
        }
        return files;
    }

    public String getCaseFileObjectKey(UUID caseUuid) {
        List<S3Object> s3ObjectSummaries = getCaseFileSummaries(caseUuid);
        if (!s3ObjectSummaries.isEmpty()) {
            return s3ObjectSummaries.get(0).key();
        } else {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No files found for case " + caseUuid);
        }
    }

    private CaseInfos infosFromDownloadCaseFileSummary(S3Object objectSummary) {
        UUID uuid = parseUuidFromKey(objectSummary.key());
        HeadObjectResponse headObjectResponse = s3Client.headObject(builder -> builder.bucket(bucketName).key(objectSummary.key()));
        String format = headObjectResponse.metadata().get("format"); // get format from metadata
        return createInfos(parseFilenameFromKey(objectSummary.key()), uuid, format);
    }

    private List<CaseInfos> infosFromDownloadCaseFileSummaries(List<S3Object> objectSummaries) {
        List<CaseInfos> caseInfosList = new ArrayList<>();
        for (S3Object objectSummary : objectSummaries) {
            UUID uuid = parseUuidFromKey(objectSummary.key());
            HeadObjectResponse headObjectResponse = s3Client.headObject(builder -> builder.bucket(bucketName).key(objectSummary.key()));
            String format = headObjectResponse.metadata().get("format");  // get format from metadata
            CaseInfos caseInfos = createInfos(parseFilenameFromKey(objectSummary.key()), uuid, format);
            caseInfosList.add(caseInfos);
        }
        return caseInfosList;
    }

    public CaseInfos getCase(UUID caseUuid) {
        var caseFileSummaries = getCaseFileSummaries(caseUuid);
        if (caseFileSummaries.isEmpty()) {
            return null;
        } else {
            return infosFromDownloadCaseFileSummary(caseFileSummaries.get(0));
        }
    }

    public String getCaseName(UUID caseUuid) {
        CaseInfos caseInfos = getCase(caseUuid);
        return caseInfos.getName();
    }

    public Optional<byte[]> getCaseBytes(UUID caseUuid) {
        String caseFileKey = getCaseFileObjectKey(caseUuid);

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(caseFileKey)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
            byte[] content = objectBytes.asByteArray();
            return Optional.of(content);
        } catch (NoSuchKeyException e) {
            // the expected key dons't exist in the backet s3
            return Optional.empty();
        } catch (S3Exception e) {
            // handle error s3
            return Optional.empty();
        }
    }

    @Override
    public List<CaseInfos> getCases() {
        List<S3Object> s3ObjectSummaries = getCasesSummaries(CASES_PREFIX);
        return infosFromDownloadCaseFileSummaries(s3ObjectSummaries);
    }

    public boolean caseExists(UUID caseName) {
        return !getCasesSummaries(uuidToPrefixKey(caseName)).isEmpty();
    }

    public UUID importCase(MultipartFile mpf, boolean withExpiration) {
        UUID caseUuid = UUID.randomUUID();

        String caseName = mpf.getOriginalFilename();
        validateCaseName(caseName);

        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);

        try (InputStream inputStream = mpf.getInputStream()) {
            String key = uuidAndFilenameToKey(caseUuid, caseName);

            Map<String, String> userMetadata = new HashMap<>();
            userMetadata.put("format", format);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(mpf.getContentType())
                    .metadata(userMetadata)
                    .build();

            // Use putObject to upload the file with metadata
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, mpf.getSize()));
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        } catch (S3Exception e) {
            throw new RuntimeException("Error uploading object to S3", e);
        }

        createCaseMetadataEntity(caseUuid, withExpiration, caseMetadataRepository);
        CaseInfos caseInfos = createInfos(caseName, caseUuid, format);
        caseInfosService.addCaseInfos(caseInfos);
        sendImportMessage(caseInfos.createMessage());

        return caseUuid;
    }

    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {

        String sourceKey = getCaseFileObjectKey(sourceCaseUuid);
        CaseInfos existingCaseInfos = caseInfosService.getCaseInfosByUuid(sourceCaseUuid.toString())
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + " not found"));
        UUID newCaseUuid = UUID.randomUUID();
        String targetKey = uuidAndFilenameToKey(newCaseUuid, parseFilenameFromKey(sourceKey));
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                .copySource(bucketName + "/" + sourceKey)
                .bucket(bucketName)
                .key(targetKey)
                .build();
        try {
            s3Client.copyObject(copyObjectRequest);
        } catch (S3Exception e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "source case " + sourceCaseUuid + " not found");
        }
        CaseInfos caseInfos = createInfos(existingCaseInfos.getName(), newCaseUuid, existingCaseInfos.getFormat());
        caseInfosService.addCaseInfos(caseInfos);
        createCaseMetadataEntity(newCaseUuid, withExpiration, caseMetadataRepository);
        sendImportMessage(caseInfos.createMessage());
        return newCaseUuid;
    }

    @Transactional
    public void disableCaseExpiration(UUID caseUuid) {
        CaseMetadataEntity caseMetadataEntity = caseMetadataRepository.findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "case " + caseUuid + " not found"));
        caseMetadataEntity.setExpirationDate(null);
    }

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

    public void deleteCase(UUID caseUuid) {
        String prefixKey = uuidToPrefixKey(caseUuid);
        List<ObjectIdentifier> objectsToDelete = s3Client.listObjectsV2(builder -> builder.bucket(bucketName).prefix(prefixKey))
            .contents()
            .stream()
            .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
            .collect(Collectors.toList());

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

    public void deleteAllCases() {
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(CASES_PREFIX)
                .build();

        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
        List<ObjectIdentifier> objectsToDelete = listObjectsResponse.contents().stream()
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .collect(Collectors.toList());

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

    public void setComputationManager(ComputationManager computationManager) {
        this.computationManager = Objects.requireNonNull(computationManager);
    }
    /*
     The query is an elasticsearch (Lucene) form query, so here it will be :
     date:XXX AND geographicalCode:(X)
     date:XXX AND geographicalCode:(X OR Y OR Z)
    */

    public List<CaseInfos> searchCases(String query) {
        return caseInfosService.searchCaseInfos(query);
    }

    private void sendImportMessage(Message<String> message) {
        OUTPUT_MESSAGE_LOGGER.debug("Sending message : {}", message);
        caseInfosPublisher.send("publishCaseImport-out-0", message);
    }

    public void reindexAllCases() {
        caseInfosService.recreateAllCaseInfos(getCases());
    }

    public List<CaseInfos> getMetadata(List<UUID> ids) {
        List<CaseInfos> cases = new ArrayList<>();
        ids.forEach(caseUuid -> {
            CaseInfos caseInfos = getCase(caseUuid);
            if (Objects.nonNull(caseInfos)) {
                cases.add(caseInfos);
            }
        });
        return cases;
    }

    @Override
    public void setFileSystem(FileSystem fileSystem) {

    }

    @Override
    public Path getCaseFile(UUID caseUuid) {
        return null;
    }

    @Override
    public void checkStorageInitialization() {

    }
}
