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
import com.powsybl.ws.commons.SecuredZipInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.FileNameUtils;
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
import org.springframework.web.multipart.MultipartFile;

import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.powsybl.caseserver.Utils.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
public class S3CaseService implements CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3CaseService.class);
    public static final int MAX_SIZE = 500000000;
    public static final String DELIMITER = "/";

    private ComputationManager computationManager = LocalComputationManager.getDefault();

    private final CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private CaseInfosService caseInfosService;

    @Autowired
    NotificationService notificationService;

    @Value("${spring.cloud.aws.bucket}")
    private String bucketName;

    @Value("${case-subpath}")
    private String rootDirectory;

    @Autowired
    private S3Client s3Client;

    public S3CaseService(CaseMetadataRepository caseMetadataRepository) {
        this.caseMetadataRepository = caseMetadataRepository;
    }

    @Override
    public String getRootDirectory() {
        return rootDirectory;
    }

    public S3Client getS3Client() {
        return s3Client;
    }

    public String getBucketName() {
        return bucketName;
    }

    String getFormat(Path caseFile) {
        Importer importer = getImporterOrThrowsException(caseFile);
        return importer.getFormat();
    }

    // creates a directory, and then in this directory, initializes a file with content.
    // After applying f to the file, deletes the file and the directory.
    private <R, T1 extends Exception, T2 extends Exception> R withTempCopy(UUID caseUuid, String filename,
                                                                                         FailableConsumer<Path, T1> contentInitializer, FailableFunction<Path, R, T2> f) {
        Path tempdirPath;
        Path tempCasePath;
        try {
            tempdirPath = Files.createTempDirectory(caseUuid.toString(), getRwxAttribute());

            // Create parent directory if necessary
            Path parentPath = Paths.get(filename).getParent();
            if (parentPath != null) {
                Files.createDirectory(tempdirPath.resolve(parentPath), getRwxAttribute());
            }
            // after this line, need to cleanup the dir
        } catch (IOException e) {
            throw CaseException.createTempDirectory(caseUuid, e);
        }
        try {
            tempCasePath = tempdirPath.resolve(filename);
            try {
                contentInitializer.accept(tempCasePath);
            } catch (Exception e) {
                throw CaseException.createUInitTempFileError(caseUuid, e);
            }
            // after this line, need to cleanup the file
            try {
                try {
                    return f.apply(tempCasePath);
                } catch (Exception e) {
                    throw CaseException.createFileNotImportable(tempdirPath.toString(), e);
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
                if (Files.exists(tempdirPath)) {
                    FileUtils.deleteDirectory(tempdirPath.toFile());
                }
            } catch (IOException e) {
                LOGGER.error("Error cleaning up temporary case directory: " + tempdirPath, e);
            }
        }
    }

    // downloads from s3 and cleanup
    public <R, T extends Exception> R withS3DownloadedTempPath(UUID caseUuid, FailableFunction<Path, R, T> f) {
        return withS3DownloadedTempPath(caseUuid, null, f);
    }

    public <R, T extends Exception> R withS3DownloadedTempPath(UUID caseUuid, String caseFileKey, FailableFunction<Path, R, T> f) {
        String nonNullCaseFileKey = Objects.requireNonNullElse(caseFileKey, uuidToKeyWithOriginalFileName(caseUuid));
        String filename = parseFilenameFromKey(nonNullCaseFileKey);
        return withTempCopy(caseUuid, filename, path ->
                        s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(nonNullCaseFileKey).build(), path), f);
    }

    @Override
    public String getFormat(UUID caseUuid) {
        return getCaseMetaDataEntity(caseUuid).getFormat();
    }

    public String getCompressionFormat(UUID caseUuid) {
        return getCaseMetaDataEntity(caseUuid).getCompressionFormat();
    }

    public String getOriginalFilename(UUID caseUuid) {
        return getCaseMetaDataEntity(caseUuid).getOriginalFilename();
    }

    // key format is "<rootDirectory>/UUID/path/to/file"
    private UUID parseUuidFromKey(String key) {
        int firstSlash = key.indexOf(DELIMITER);
        int secondSlash = key.indexOf(DELIMITER, firstSlash + 1);
        return UUID.fromString(key.substring(firstSlash + 1, secondSlash));
    }

    private String parseFilenameFromKey(String key) {
        int firstSlash = key.indexOf(DELIMITER);
        int secondSlash = key.indexOf(DELIMITER, firstSlash + 1);
        return key.substring(secondSlash + 1);
    }

    public String uuidToKeyPrefix(UUID uuid) {
        return rootDirectory + DELIMITER + uuid.toString() + DELIMITER;
    }

    public String uuidToKeyWithFileName(UUID uuid, String filename) {
        return uuidToKeyPrefix(uuid) + filename;
    }

    public String uuidToKeyWithOriginalFileName(UUID caseUuid) {
        if (Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
            return uuidToKeyWithFileName(caseUuid, getOriginalFilename(caseUuid) + GZIP_EXTENSION);
        }
        return uuidToKeyWithFileName(caseUuid, getOriginalFilename(caseUuid));
    }

    private List<S3Object> getCaseS3Objects(String keyPrefix) {
        List<S3Object> s3Objects = new ArrayList<>();
        ListObjectsV2Iterable listObjectsV2Iterable = s3Client.listObjectsV2Paginator(getListObjectsV2Request(keyPrefix));
        listObjectsV2Iterable.iterator().forEachRemaining(listObjectsChunk ->
            s3Objects.addAll(listObjectsChunk.contents())
        );
        return s3Objects;
    }

    private ListObjectsV2Request getListObjectsV2Request(String prefix) {
        return ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build();
    }

    private List<S3Object> getCaseS3Objects(UUID caseUuid) {
        return getCaseS3Objects(uuidToKeyPrefix(caseUuid));
    }

    @Override
    public CaseInfos getCaseInfos(UUID caseUuid) {
        if (!caseExists(caseUuid)) {
            LOGGER.error("The directory with the following uuid doesn't exist: {}", caseUuid);
            return null;
        }
        return new CaseInfos(caseUuid, getCaseName(caseUuid), getFormat(caseUuid));
    }

    @Override
    public String getCaseName(UUID caseUuid) {
        String originalFilename = getOriginalFilename(caseUuid);
        if (originalFilename == null) {
            throw CaseException.createOriginalFileNotFound(caseUuid);
        }
        return originalFilename;
    }

    @Override
    public Optional<InputStream> getCaseStream(UUID caseUuid) {
        String caseFileKey = null;
        try {
            caseFileKey = uuidToKeyWithOriginalFileName(caseUuid);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(caseFileKey)
                    .build();

            ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
            if (Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
                return Optional.of(new GZIPInputStream(responseInputStream));
            }
            return Optional.of(responseInputStream);
        } catch (NoSuchKeyException e) {
            LOGGER.error("The expected key does not exist in the bucket s3 : {}", caseFileKey);
            return Optional.empty();
        } catch (CaseException | ResponseStatusException e) {
            LOGGER.error(e.getMessage());
            return Optional.empty();
        } catch (IOException e) {
            LOGGER.error("Unable to decompress {}", caseFileKey);
            return Optional.empty();
        }
    }

    @Override
    public List<CaseInfos> getCases() {
        List<CaseInfos> caseInfosList = new ArrayList<>();
        CaseInfos caseInfos;
        for (S3Object o : getCaseS3Objects(rootDirectory + DELIMITER)) {
            caseInfos = getCaseInfoSafely(o.key());
            if (Objects.nonNull(caseInfos)) {
                caseInfosList.add(caseInfos);
            }
        }
        return caseInfosList;
    }

    private CaseInfos getCaseInfoSafely(String key) {
        Objects.requireNonNull(key);
        try {
            return getCaseInfos(parseUuidFromKey(key));
        } catch (Exception e) {
            // This method is called by getCases() that is a method for supervision and administration. We do not want the request to stop and fail on error cases.
            LOGGER.error("Error processing file {}: {}", key, e.getMessage(), e);
            return null;
        }
    }

    @Override
    public boolean caseExists(UUID uuid) {
        return !getCaseS3Objects(uuid).isEmpty();
    }

    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        if (getCaseS3Objects(caseUuid).size() > 1 && fileName.equals(getCaseName(caseUuid))) {
            return Boolean.FALSE;
        }

        String key = uuidToKeyWithFileName(caseUuid, fileName);
        String caseName = getCaseName(caseUuid);
        // For compressed cases, we append the compression extension to the case name as only the compressed file is stored in S3.
        // i.e. : Assuming test.xml.gz is stored in S3. When you request datasourceExists(randomUUID, "test.xml"), you ask to S3 API ("test.xml" + ".gz") exists ? => true
        if (isCompressedCaseFile(caseName)) {
            key = key + "." + getCompressionFormat(caseUuid);
        } else if (isArchivedCaseFile(caseName) || Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
            key = key + GZIP_EXTENSION;
        }

        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();
        try {
            s3Client.headObject(headObjectRequest);
            return Boolean.TRUE;
        } catch (NoSuchKeyException e) {
            return Boolean.FALSE;
        }
    }

    public Set<String> listName(UUID caseUuid, String regex) {
        List<String> filenames;
        String originalFilename = getOriginalFilename(caseUuid);
        if (isCompressedCaseFile(originalFilename)) {
            // For a compressed file basename.xml.gz, listName() should return ['basename.xml']. That's why we remove the compression extension to the filename.
            filenames = List.of(removeExtension(originalFilename, "." + getCompressionFormat(caseUuid)));
        } else if (Boolean.TRUE.equals(isUploadedAsPlainFile(caseUuid))) {
            // for files that are not compressed when imported (but are in the back)
            filenames = List.of(removeExtension(originalFilename, GZIP_EXTENSION));
        } else {
            List<S3Object> s3Objects = getCaseS3Objects(caseUuid);
            filenames = s3Objects.stream().map(obj -> Paths.get(obj.key()).toString().replace(rootDirectory + DELIMITER + caseUuid.toString() + DELIMITER, "")).toList();
            // For archived cases :
            if (isArchivedCaseFile(originalFilename)) {
                filenames = filenames.stream()
                        // the original archive name has to be filtered.
                        .filter(name -> !name.equals(originalFilename))
                        // each subfile hase been gzipped -> we have to remove the gz extension (only one, the one we added).
                        .map(name -> removeExtension(name, GZIP_EXTENSION))
                        .collect(Collectors.toList());
            }
        }
        return filenames.stream().filter(n -> n.matches(regex)).collect(Collectors.toSet());
    }

    @Override
    public UUID importCase(MultipartFile mpf, boolean withExpiration, boolean withIndexation, UUID caseUuid) {
        String caseName = Objects.requireNonNull(mpf.getOriginalFilename());
        validateCaseName(caseName);

        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);
        String compressionFormat = FileNameUtils.getExtension(Paths.get(caseName));

        try (InputStream inputStream = mpf.getInputStream()) {
            // We store archived cases in S3 in a specific way : in the caseUuid directory, we store :
            // - the original archive
            // - the extracted files are exploded in the caseUuid directory. This allows to use HeadObjectRequest for datasource/exists,
            // to download subfiles separately, or to anwser to datasource/list with ListObjectV2.
            // But this unarchived storage could increase tenfold used disk space: so each extracted file is gzipped to avoid increasing it.
            // Compression of subfiles is done in a simple way: no matter if the subfile is compressed or not, it will be gzipped in the storage.
            // example : archive.zip containing [file1.xml, file2.xml.gz]
            //           will be stored as :
            //              - archive.zip
            //              - file1.xml.gz
            //              - file2.xml.gz.gz
            if (isZippedFile(caseName)) {
                importZipContent(mpf.getInputStream(), caseUuid);
            } else if (isTaredFile(caseName)) {
                importTarContent(mpf.getInputStream(), caseUuid);
            }
            RequestBody requestBody;
            String contentType = mpf.getContentType();
            String fileName = caseName;
            if (!isArchivedCaseFile(caseName) && !isCompressedCaseFile(caseName)) {
                // plain files only
                fileName += GZIP_EXTENSION;
                contentType = "application/octet-stream";
                Path tempFile = writeGzipTmpFileOnFileSystem(inputStream, "plain-file-", ".tmp");
                uploadToS3(uuidToKeyWithFileName(caseUuid, fileName), contentType, RequestBody.fromFile(tempFile));
                Files.deleteIfExists(tempFile);
            } else {
                // archived files and already compressed files
                requestBody = RequestBody.fromInputStream(inputStream, mpf.getSize());
                uploadToS3(uuidToKeyWithFileName(caseUuid, fileName), contentType, requestBody);
            }
        } catch (IOException e) {
            throw CaseException.createFileNotImportable(caseName, e);
        }

        createCaseMetadataEntity(caseUuid, withExpiration, withIndexation, caseName, compressionFormat, format);
        CaseInfos caseInfos = createInfos(caseName, caseUuid, format);
        if (withIndexation) {
            caseInfosService.addCaseInfos(caseInfos);
        }
        notificationService.sendImportMessage(caseInfos.createMessage());

        return caseUuid;
    }

    private void uploadToS3(String key, String contentType, RequestBody requestBody) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType(contentType)
                .build();
        s3Client.putObject(putObjectRequest, requestBody);
    }

    private Path writeGzipTmpFileOnFileSystem(InputStream inputStream, String prefix, String suffix) throws IOException {
        Path tempFile = Files.createTempFile(prefix, suffix, getRwxAttribute());
        try (OutputStream fileOutputStream = Files.newOutputStream(tempFile);
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream)) {
            inputStream.transferTo(gzipOutputStream);
        }
        return tempFile;
    }

    private void importZipContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (ZipInputStream zipInputStream = new SecuredZipInputStream(inputStream, 1000, MAX_SIZE)) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    processZipEntry(caseUuid, zipInputStream, entry);
                }
                zipInputStream.closeEntry();
            }
        }
    }

    private void importTarContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (BufferedInputStream tarStream = new BufferedInputStream(inputStream);
            TarArchiveInputStream tarInputStream = new TarArchiveInputStream(tarStream)) {
            ArchiveEntry entry;
            while ((entry = tarInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    processTarEntry(caseUuid, tarInputStream, entry);
                }
            }
        }
    }

    private void copyZipContent(InputStream inputStream, UUID sourcecaseUuid, UUID caseUuid) throws IOException {
        try (ZipInputStream zipInputStream = new SecuredZipInputStream(inputStream, 1000, MAX_SIZE)) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    copyEntry(sourcecaseUuid, caseUuid, entry.getName() + GZIP_EXTENSION);
                }
                zipInputStream.closeEntry();
            }
        }
    }

    private void copyTarContent(InputStream inputStream, UUID sourcecaseUuid, UUID caseUuid) throws IOException {
        try (BufferedInputStream tarStream = new BufferedInputStream(inputStream);
             TarArchiveInputStream tarInputStream = new TarArchiveInputStream(tarStream)) {
            ArchiveEntry entry;
            while ((entry = tarInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    copyEntry(sourcecaseUuid, caseUuid, entry.getName() + GZIP_EXTENSION);
                }
            }
        }
    }

    private void copyEntry(UUID sourcecaseUuid, UUID caseUuid, String fileName) {
        // To optimize copy, files to copy are not downloaded on the case-server. They are directly copied on the S3 server.
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(rootDirectory + DELIMITER + sourcecaseUuid + DELIMITER + fileName)
                .destinationBucket(bucketName)
                .destinationKey(rootDirectory + DELIMITER + caseUuid + DELIMITER + fileName)
                .build();
        try {
            s3Client.copyObject(copyObjectRequest);
        } catch (S3Exception e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source file " + caseUuid + DELIMITER + fileName + NOT_FOUND);
        }
    }

    private void processZipEntry(UUID caseUuid, ZipInputStream zipInputStream, ZipEntry entry) throws IOException {
        String fileName = entry.getName();
        String extractedKey = uuidToKeyWithFileName(caseUuid, fileName);
        Path tempFile = writeGzipTmpFileOnFileSystem(zipInputStream, "compressed-zip-", ".gz");
        uploadToS3(extractedKey + GZIP_EXTENSION, Files.probeContentType(Paths.get(fileName)), RequestBody.fromFile(tempFile));
        Files.deleteIfExists(tempFile);
    }

    private void processTarEntry(UUID caseUuid, TarArchiveInputStream tarInputStream, ArchiveEntry entry) throws IOException {
        String fileName = entry.getName();
        String extractedKey = uuidToKeyWithFileName(caseUuid, fileName);
        Path tempFile = writeGzipTmpFileOnFileSystem(tarInputStream, "compressed-tar-", ".gz");
        uploadToS3(extractedKey + GZIP_EXTENSION, Files.probeContentType(Paths.get(fileName)), RequestBody.fromFile(tempFile));
        Files.deleteIfExists(tempFile);
    }

    @Override
    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {
        if (!caseExists(sourceCaseUuid)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + NOT_FOUND);
        }

        String sourceKey = uuidToKeyWithOriginalFileName(sourceCaseUuid);
        UUID newCaseUuid = UUID.randomUUID();
        String targetKey = uuidToKeyWithFileName(newCaseUuid, parseFilenameFromKey(sourceKey));
        // To optimize copy, cases to copy are not downloaded on the case-server. They are directly copied on the S3 server.
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
        CaseMetadataEntity existingCase = getCaseMetaDataEntity(sourceCaseUuid);
        createCaseMetadataEntity(newCaseUuid, withExpiration, existingCase.isIndexed(), existingCase.getOriginalFilename(), existingCase.getCompressionFormat(), existingCase.getFormat());
        Optional<InputStream> caseStream = getCaseStream(newCaseUuid);
        if (caseStream.isPresent() && isArchivedCaseFile(existingCase.getOriginalFilename())) {
            if (isZippedFile(existingCase.getOriginalFilename())) {
                try {
                    copyZipContent(caseStream.get(), sourceCaseUuid, newCaseUuid);
                } catch (Exception e) {
                    throw CaseException.createCopyZipContentError(sourceCaseUuid, e);
                }
            } else if (isTaredFile(existingCase.getOriginalFilename())) {
                try {
                    copyTarContent(caseStream.get(), sourceCaseUuid, newCaseUuid);
                } catch (Exception e) {
                    throw CaseException.createCopyZipContentError(sourceCaseUuid, e);
                }
            }
        }
        CaseInfos existingCaseInfos = getCaseInfos(sourceCaseUuid);
        CaseInfos caseInfos = createInfos(existingCaseInfos.getName(), newCaseUuid, existingCaseInfos.getFormat());
        if (existingCase.isIndexed()) {
            caseInfosService.addCaseInfos(caseInfos);
        }
        notificationService.sendImportMessage(caseInfos.createMessage());
        return newCaseUuid;
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
        String prefixKey = uuidToKeyPrefix(caseUuid);
        List<ObjectIdentifier> objectsToDelete = s3Client.listObjectsV2(builder -> builder.bucket(bucketName).prefix(prefixKey))
            .contents()
            .stream()
            .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
            .toList();

        if (!objectsToDelete.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(delete -> delete.objects(objectsToDelete))
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
                .prefix(rootDirectory + DELIMITER)
                .build();

        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);
        List<ObjectIdentifier> objectsToDelete = listObjectsResponse.contents().stream()
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .toList();

        if (!objectsToDelete.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(delete -> delete.objects(objectsToDelete))
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

    @Override
    public ComputationManager getComputationManager() {
        return computationManager;
    }

    @Override
    public CaseMetadataRepository getCaseMetadataRepository() {
        return caseMetadataRepository;
    }

}
