/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.google.common.io.ByteStreams;
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
import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
public class S3CaseService implements CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3CaseService.class);
    public static final int MAX_SIZE = 500000000;
    public static final List<String> COMPRESSION_FORMATS = List.of("bz2", "gz", "xz", "zst");
    public static final List<String> ARCHIVE_FORMATS = List.of("zip");
    public static final String DELIMITER = "/";
    public static final String GZIP_EXTENSION = ".gz";

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
        Importer importer = getImporterOrThrowsException(caseFile);
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

            // Create parent directory if necessary
            Path parentPath = Paths.get(filename).getParent();
            if (parentPath != null) {
                Files.createDirectory(tempdirPath.resolve(parentPath), attr);
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
                throw CaseException.initTempFile(caseUuid, ex);
            }
            // after this line, need to cleanup the file
            try {
                try {
                    return f.apply(tempCasePath);
                } catch (CaseException e) {
                    throw CaseException.createFileNotImportable(tempdirPath);
                } catch (Throwable t) {
                    throw CaseException.processTempFile(caseUuid, t);
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
    public <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, FailableFunction<Path, R, T> f) {
        return withS3DownloadedTempPath(caseUuid, null, f);
    }

    public <R, T extends Throwable> R withS3DownloadedTempPath(UUID caseUuid, String caseFileKey, FailableFunction<Path, R, T> f) {
        String nonNullCaseFileKey = Objects.requireNonNullElse(caseFileKey, uuidToKeyWithOriginameFileName(caseUuid));
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

    // key format is "gsi-cases/UUID/filename"
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

    public static String uuidToKeyPrefix(UUID uuid) {
        return CASES_PREFIX + uuid.toString() + DELIMITER;
    }

    public static String uuidToKeyWithFileName(UUID uuid, String filename) {
        return uuidToKeyPrefix(uuid) + filename;
    }

    public String uuidToKeyWithOriginameFileName(UUID caseUuid) {
        return uuidToKeyWithFileName(caseUuid, getOriginalFilename(caseUuid));
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
        List<S3Object> files = getCasesSummaries(uuidToKeyPrefix(caseUuid));
        if (files.size() > 1) {
            LOGGER.warn("Multiple files for case {}", caseUuid);
        }
        return files;
    }

    private List<CaseInfos> infosFromDownloadCaseFileSummaries(List<S3Object> objectSummaries) {
        List<CaseInfos> caseInfosList = new ArrayList<>();
        for (S3Object objectSummary : objectSummaries) {
            final var caseInfo = getCaseInfos(parseUuidFromKey(objectSummary.key()));
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
        String originalFilename = getOriginalFilename(caseUuid);
        if (originalFilename == null) {
            throw CaseException.createOriginalFileNotFound(caseUuid);
        }
        return originalFilename;
    }

    @Override
    public Optional<byte[]> getCaseBytes(UUID caseUuid) {
        String caseFileKey = null;
        try {
            caseFileKey = uuidToKeyWithOriginameFileName(caseUuid);
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(caseFileKey)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
            return Optional.of(objectBytes.asByteArray());
        } catch (NoSuchKeyException e) {
            LOGGER.error("The expected key does not exist in the bucket s3 : {}", caseFileKey);
            return Optional.empty();
        } catch (CaseException | ResponseStatusException e) {
            LOGGER.error(e.getMessage());
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
        return !getCasesSummaries(uuidToKeyPrefix(uuid)).isEmpty();
    }

    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        if (getCaseFileSummaries(caseUuid).size() > 1 && fileName.equals(getCaseName(caseUuid))) {
            return Boolean.FALSE;
        }

        String key = uuidToKeyWithFileName(caseUuid, fileName);
        String caseName = getCaseName(caseUuid);
        // For compressed cases, we append the compression extension to the case name as only the compressed file is stored in S3.
        // i.e. : Assuming test.xml.gz is stored in S3. When you request datasourceExists(randomUUID, "test.xml"), you ask to S3 API ("test.xml" + ".gz") exists ? => true
        if (isCompressedCaseFile(caseName)) {
            key = key + "." + getCompressionFormat(caseUuid);
        } else if (isArchivedCaseFile(caseName)) {
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

    public static boolean isCompressedCaseFile(String caseName) {
        return COMPRESSION_FORMATS.stream().anyMatch(cf -> caseName.endsWith("." + cf));
    }

    public static boolean isArchivedCaseFile(String caseName) {
        return ARCHIVE_FORMATS.stream().anyMatch(cf -> caseName.endsWith("." + cf));
    }

    private static String removeExtension(String filename, String extension) {
        int index = filename.lastIndexOf(extension);
        if (index == -1) {
            return filename;
        } else if(index + extension.length() != filename.length()) {
            //extension to remove is not at the end
            throw CaseException.createIllegalCaseName(filename);
        }
        return filename.substring(0, index);
    }

    public Set<String> listName(UUID caseUuid, String regex) {
        List<String> fileNames;
        if (isCompressedCaseFile(getOriginalFilename(caseUuid))) {
            // For a compressed file basename.xml.gz, listName() should return ['basename.xml']. That's why we remove the compression extension to the filename.
            fileNames = List.of(removeExtension(getOriginalFilename(caseUuid), "." + getCompressionFormat(caseUuid)));
        } else {
            List<S3Object> s3Objects = getCaseFileSummaries(caseUuid);
            fileNames = s3Objects.stream().map(obj -> Paths.get(obj.key()).toString().replace(CASES_PREFIX + caseUuid.toString() + DELIMITER, "")).toList();
            // For archived cases :
            if (isArchivedCaseFile(getOriginalFilename(caseUuid))) {
                // the original archive name has to be filtered.
                fileNames = fileNames.stream().filter(name -> !name.equals(getOriginalFilename(caseUuid))).toList();
                // each subfile hase been gzipped -> we have to remove the gz extension (only one, the one we added).
                fileNames = fileNames.stream().map(name -> removeExtension(name, GZIP_EXTENSION)).toList();
            }
        }
        return fileNames.stream().filter(n -> n.matches(regex)).collect(Collectors.toSet());
    }

    @Override
    public UUID importCase(MultipartFile mpf, boolean withExpiration, boolean withIndexation) {
        UUID caseUuid = UUID.randomUUID();

        String caseName = Objects.requireNonNull(mpf.getOriginalFilename());
        validateCaseName(caseName);

        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);
        String compressionFormat = FileNameUtils.getExtension(Paths.get(caseName));

        try (InputStream inputStream = mpf.getInputStream()) {
            String key = uuidToKeyWithFileName(caseUuid, caseName);

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
            if (isArchivedCaseFile(caseName)) {
                importZipContent(mpf.getInputStream(), caseUuid);
            }

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(mpf.getContentType())
                    .build();
            // Use putObject to upload the file
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, mpf.getSize()));

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

    private void importZipContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (ZipInputStream zipInputStream = new SecuredZipInputStream(inputStream, 1000, MAX_SIZE)) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    processEntry(caseUuid, zipInputStream, entry);
                }
                zipInputStream.closeEntry();
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

    private void copyEntry(UUID sourcecaseUuid, UUID caseUuid, String fileName) {
        // To optimize copy, files to copy are not downloaded on the case-server. They are directly copied on the S3 server.
        CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                .sourceBucket(bucketName)
                .sourceKey(CASES_PREFIX + sourcecaseUuid + "/" + fileName)
                .destinationBucket(bucketName)
                .destinationKey(CASES_PREFIX + caseUuid + "/" + fileName)
                .build();
        try {
            s3Client.copyObject(copyObjectRequest);
        } catch (S3Exception e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source file " + caseUuid + "/" + fileName + NOT_FOUND);
        }
    }

    private void processEntry(UUID caseUuid, ZipInputStream zipInputStream, ZipEntry entry) throws IOException {
        String fileName = entry.getName();
        String extractedKey = uuidToKeyWithFileName(caseUuid, fileName);
        byte[] fileBytes = compress(ByteStreams.toByteArray(zipInputStream));

        PutObjectRequest extractedFileRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(extractedKey + GZIP_EXTENSION)
                .contentType(Files.probeContentType(Paths.get(fileName))) // Detect the MIME type
                .build();
        s3Client.putObject(extractedFileRequest, RequestBody.fromBytes(fileBytes));
    }

    private static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(data, 0, data.length);
        gzipOutputStream.close();
        return outputStream.toByteArray();
    }

    public static byte[] decompress(byte[] data) throws IOException {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(data));
        return IOUtils.toByteArray(gzipInputStream);
    }

    @Override
    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {
        if (!caseExists(sourceCaseUuid)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + NOT_FOUND);
        }

        String sourceKey = uuidToKeyWithOriginameFileName(sourceCaseUuid);
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
        Optional<byte[]> caseBytes = getCaseBytes(newCaseUuid);
        if (caseBytes.isPresent() && isArchivedCaseFile(existingCase.getOriginalFilename())) {
            try {
                copyZipContent(new ByteArrayInputStream(caseBytes.get()), sourceCaseUuid, newCaseUuid);
            } catch (Exception e) {
                throw CaseException.copyZipContent(sourceCaseUuid, e);
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

    @Override
    public ComputationManager getComputationManager() {
        return computationManager;
    }

    @Override
    public CaseMetadataRepository getCaseMetadataRepository() {
        return caseMetadataRepository;
    }

    private CaseMetadataEntity getCaseMetaDataEntity(UUID caseUuid) {
        return caseMetadataRepository.findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "case " + caseUuid + NOT_FOUND));
    }

}
