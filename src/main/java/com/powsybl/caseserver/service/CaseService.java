/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.google.re2j.Pattern;
import com.powsybl.caseserver.CaseException;
import com.powsybl.caseserver.datasource.utils.S3MultiPartFile;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.parsers.FileNameInfos;
import com.powsybl.caseserver.parsers.FileNameParser;
import com.powsybl.caseserver.parsers.FileNameParsers;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.computation.ComputationManager;
import com.powsybl.computation.local.LocalComputationManager;
import com.powsybl.iidm.network.Importer;
import com.powsybl.ws.commons.SecuredTarInputStream;
import com.powsybl.ws.commons.SecuredZipInputStream;
import lombok.Getter;
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
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.powsybl.caseserver.Utils.*;
import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM_VALUE;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
public class CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaseService.class);
    public static final int MAX_UNCOMPRESSED_ARCHIVE_SIZE = 2000000000;
    public static final int MAX_ARCHIVE_ENTRIES = 1000;
    public static final String DELIMITER = "/";

    @Getter
    private ComputationManager computationManager = LocalComputationManager.getDefault();

    @Getter
    @Autowired
    private final CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private final CaseObserver caseObserver;

    @Autowired
    private CaseInfosService caseInfosService;

    @Autowired
    NotificationService notificationService;

    @Value("${spring.cloud.aws.bucket}")
    private String bucketName;

    @Value("${powsybl-ws.s3.subpath.prefix:}${case-subpath}")
    private String rootDirectory;

    @Autowired
    private S3Client s3Client;

    public CaseService(CaseMetadataRepository caseMetadataRepository, CaseObserver caseObserver) {
        this.caseMetadataRepository = caseMetadataRepository;
        this.caseObserver = caseObserver;
    }

    void validateCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        if (!caseName.matches("[^<>:\"/|?*]+(\\.[\\w]+)")) {
            throw CaseException.createIllegalCaseName(caseName);
        }
    }

    public CaseMetadataEntity getCaseMetaDataEntity(UUID caseUuid) {
        return getCaseMetadataRepository().findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Metadata of case " + caseUuid + NOT_FOUND));
    }

    public Boolean isUploadedAsPlainFile(UUID caseUuid) {
        String name = getCaseMetaDataEntity(caseUuid).getOriginalFilename();
        return name != null && !isCompressedCaseFile(name) && !isArchivedCaseFile(name);
    }

    public CaseInfos createInfos(String fileBaseName, UUID caseUuid, String format) {
        FileNameParser parser = FileNameParsers.findParser(fileBaseName);
        if (parser != null) {
            Optional<? extends FileNameInfos> fileNameInfos = parser.parse(fileBaseName);
            if (fileNameInfos.isPresent()) {
                return CaseInfos.create(fileBaseName, caseUuid, format, fileNameInfos.get());
            }
        }
        return CaseInfos.builder().name(fileBaseName).uuid(caseUuid).format(format).build();
    }

    public void createCaseMetadataEntity(UUID newCaseUuid, boolean withExpiration, boolean withIndexation, String originalFilename, String compressionFormat, String format) {
        Instant expirationTime = null;
        if (withExpiration) {
            expirationTime = Instant.now().plus(1, ChronoUnit.HOURS);
        }
        getCaseMetadataRepository().save(new CaseMetadataEntity(newCaseUuid, expirationTime, withIndexation, originalFilename, compressionFormat, format));
    }

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

    public Importer getImporterOrThrowsException(Path caseFile) {
        DataSource dataSource = DataSource.fromPath(caseFile);
        Importer importer = Importer.find(dataSource, getComputationManager());
        if (importer == null) {
            throw CaseException.createFileNotImportable(caseFile);
        }
        return importer;
    }

    public List<CaseInfos> getCasesToReindex() {
        Set<UUID> casesToReindex = getCaseMetadataRepository().findAllByIndexedTrue()
                .stream()
                .map(CaseMetadataEntity::getId)
                .collect(Collectors.toSet());
        return getCases().stream().filter(c -> casesToReindex.contains(c.getUuid())).toList();
    }

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
            FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
            tempdirPath = Files.createTempDirectory(caseUuid.toString(), attr);

            // Create parent directory if necessary
            Path parentPath = Paths.get(filename).getParent();
            if (parentPath != null) {
                Files.createDirectories(tempdirPath.resolve(parentPath), attr);
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
                throw CaseException.createInitTempFileError(caseUuid, e);
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
        String keyWithoutRootDirectory = key.replaceAll(rootDirectory + DELIMITER, "");
        int firstSlash = keyWithoutRootDirectory.indexOf(DELIMITER);
        return UUID.fromString(keyWithoutRootDirectory.substring(0, firstSlash));
    }

    public Optional<InputStream> getCaseStream(UUID caseUuid) {
        try {
            return getCaseStream(uuidToKeyWithOriginalFileName(caseUuid));
        } catch (CaseException | ResponseStatusException e) {
            LOGGER.error(e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<InputStream> getCaseStream(String caseFileKey) {
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(caseFileKey)
                .build();

            ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(getObjectRequest);
            return Optional.of(responseInputStream);
        } catch (NoSuchKeyException e) {
            LOGGER.error("The expected key does not exist in the bucket s3 : {}", caseFileKey);
            return Optional.empty();
        }
    }

    private String parseFilenameFromKey(String key) {
        String keyWithoutRootDirectory = key.replaceAll(rootDirectory + DELIMITER, "");
        int firstSlash = keyWithoutRootDirectory.indexOf(DELIMITER);
        return keyWithoutRootDirectory.substring(firstSlash + 1);
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

    @SuppressWarnings("javasecurity:S5145")
    public CaseInfos getCaseInfos(UUID caseUuid) {
        if (!caseExists(caseUuid)) {
            LOGGER.error("The directory with the following uuid doesn't exist: {}", caseUuid);
            return null;
        }
        return new CaseInfos(caseUuid, getCaseName(caseUuid), getFormat(caseUuid));
    }

    public String getCaseName(UUID caseUuid) {
        String originalFilename = getOriginalFilename(caseUuid);
        if (originalFilename == null) {
            throw CaseException.createOriginalFileNotFound(caseUuid);
        }
        return originalFilename;
    }

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

    public boolean caseExists(UUID uuid) {
        return caseMetadataRepository.findById(uuid).isPresent();
    }

    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        String key = uuidToKeyWithFileName(caseUuid, fileName);
        String caseName = getCaseName(caseUuid);
        // For compressed cases, we append the compression extension to the case name as only the compressed file is stored in S3.
        // i.e. : Assuming test.xml.gz is stored in S3. When you request datasourceExists(randomUUID, "test.xml"), you ask to S3 API ("test.xml" + ".gz") exists ? => true
        if (isCompressedCaseFile(caseName)) {
            key = key + "." + getCompressionFormat(caseUuid);
        } else if (isArchivedCaseFile(caseName) && fileName.equals(getCaseName(caseUuid))) {
            // We store the archive in addition to its content files, so exists when matching the archive name should return false
            return Boolean.FALSE;
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
            filenames = s3Objects.stream().map(obj -> parseFilenameFromKey(obj.key())).toList();
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
        return filenames.stream().filter(n -> Pattern.compile(regex).matches(n)).collect(Collectors.toSet());
    }

    /**
     * We store archived cases in S3 in a specific way: within the caseUuid directory, we store:
     * <ul>
     *     <li>The original archive.</li>
     *     <li>The extracted files, which are placed directly in the caseUuid directory.</li>
     * </ul>
     *
     * <p>
     * This allows us to use {@code HeadObjectRequest} to check if a datasource exists,
     * download subfiles separately, or respond to {@code datasource/list} using {@code ListObjectV2}.
     * However, this unarchived storage could significantly increase disk space usage.
     * To mitigate this, each extracted file is gzipped, regardless of whether it was already compressed.
     * Compression is applied uniformly: even if a subfile is already compressed, it will still be gzipped in storage.
     * </p>
     *
     * <p>
     * Currently, there is no optimization for archives (ZIP or TAR) containing a single file.
     * In this case, we unnecessarily decompress and recompress the file as GZIP, impacting import performance.
     * To optimize this, we could store an empty file in S3 with the name of the file inside the archive,
     * along with a boolean flag in Postgres. This would allow us to use a HEAD request to check for existence
     * and directly read the original archive stream when needed.
     * However, this approach introduces complexity:
     * - The system would behave differently when the original archive is missing making the datasource unusable in this case,
     *   whereas in other cases, the archive is only needed for re-downloading.
     * - Alternative solutions (e.g., storing the filename in Postgres or as S3 metadata) would further increase system complexity
     *   by requiring a database query or an S3 metadata retrieval instead of a simple HEAD request which could behave (in particular in case of failures) differently.
     * Since performance is currently acceptable, we haven't implemented this optimization yet.
     * </p>
     *
     * <p><b>Example:</b></p>
     * archive.zip containing [file1.xml, file2.xml.gz] will be stored as:
     * <ul>
     *     <li>archive.zip</li>
     *     <li>file1.xml.gz</li>
     *     <li>file2.xml.gz.gz</li>
     * </ul>
     */
    public UUID importCase(MultipartFile mpf, boolean withExpiration, boolean withIndexation, UUID caseUuid) {
        String caseName = Objects.requireNonNull(mpf.getOriginalFilename());
        validateCaseName(caseName);

        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);
        String compressionFormat = FileNameUtils.getExtension(Paths.get(caseName));

        // Process and store GZ compressed files extracted from archive file
        if (isArchivedCaseFile(caseName)) {
            try (InputStream inputStream = mpf.getInputStream()) {
                if (isZippedFile(caseName)) {
                    importZipContent(inputStream, caseUuid);
                } else if (isTaredFile(caseName)) {
                    importTarContent(inputStream, caseUuid);
                }
            } catch (IOException e) {
                throw CaseException.createFileNotImportable(caseName, e);
            }
        }

        // Store the original file
        try (InputStream inputStream = mpf.getInputStream()) {
            if (!isArchivedCaseFile(caseName) && !isCompressedCaseFile(caseName)) {
                // If it's a plain file, compress it before storing
                compressAndUploadToS3(caseUuid, caseName + GZIP_EXTENSION, APPLICATION_OCTET_STREAM_VALUE, inputStream);
            } else {
                // If the file is an archive or already compressed, store it as-is
                uploadToS3(
                        uuidToKeyWithFileName(caseUuid, caseName),
                        mpf.getContentType(),
                        RequestBody.fromInputStream(inputStream, mpf.getSize()));
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

    public UUID importCase(String folderKey, String contentType, boolean withExpiration, boolean withIndexation) throws IOException {

        try (S3MultiPartFile mpf = new S3MultiPartFile(this, folderKey, contentType)) {
            return importCase(mpf, withExpiration, withIndexation, UUID.randomUUID());
        }
    }

    private void compressAndUploadToS3(UUID caseUuid, String fileName, String contentType, InputStream inputStream) {
        withTempCopy(
                caseUuid,
                "tmp-" + caseUuid + ".gz",
                tempCasePath -> {
                    try {
                        writeGzTmpFileOnFileSystem(inputStream, tempCasePath);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                tempCasePath -> {
                    uploadToS3(
                            uuidToKeyWithFileName(caseUuid, fileName),
                            contentType,
                            RequestBody.fromFile(tempCasePath)
                    );
                    // The return of the method is not used here
                    return null;
                }
        );
    }

    private void uploadToS3(String key, String contentType, RequestBody requestBody) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType(contentType)
                .build();
        caseObserver.observeCaseWriting(() -> s3Client.putObject(putObjectRequest, requestBody));
    }

    private void writeGzTmpFileOnFileSystem(InputStream inputStream, Path tempCasePath) throws IOException {
        try (OutputStream fileOutputStream = Files.newOutputStream(tempCasePath);
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream)) {
            inputStream.transferTo(gzipOutputStream);
        }
    }

    private void importZipContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (ZipInputStream zipInputStream = new SecuredZipInputStream(inputStream, MAX_ARCHIVE_ENTRIES, MAX_UNCOMPRESSED_ARCHIVE_SIZE)) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    processCompressedEntry(caseUuid, zipInputStream, entry.getName());
                }
                zipInputStream.closeEntry();
            }
        }
    }

    private void importTarContent(InputStream inputStream, UUID caseUuid) throws IOException {
        try (TarArchiveInputStream tarInputStream = new SecuredTarInputStream(inputStream, MAX_ARCHIVE_ENTRIES, MAX_UNCOMPRESSED_ARCHIVE_SIZE)) {
            ArchiveEntry entry;
            while ((entry = tarInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    processCompressedEntry(caseUuid, tarInputStream, entry.getName());
                }
            }
        }
    }

    private <T extends InputStream> void processCompressedEntry(UUID caseUuid, T compressedInputStream, String fileName) throws IOException {
        compressAndUploadToS3(
                caseUuid,
                fileName + GZIP_EXTENSION,
                Files.probeContentType(Paths.get(fileName)), // Detect the MIME type
                compressedInputStream);
    }

    public UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {
        if (!caseExists(sourceCaseUuid)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + NOT_FOUND);
        }

        UUID newCaseUuid = UUID.randomUUID();
        ListObjectsResponse sourceCaseObjects = s3Client.listObjects(ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(uuidToKeyPrefix(sourceCaseUuid))
                .build()
        );
        if (sourceCaseObjects.contents().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "The expected key does not exist in the bucket s3 : " + uuidToKeyPrefix(sourceCaseUuid));
        }

        // To optimize copy, cases to copy are not downloaded on the case-server. They are directly copied on the S3 server.
        for (S3Object object : sourceCaseObjects.contents()) {
            String filename = parseFilenameFromKey(object.key());
            String sourceKey = uuidToKeyWithFileName(sourceCaseUuid, filename);
            String targetKey = uuidToKeyWithFileName(newCaseUuid, filename);
            CopyObjectRequest copyObjectRequest = CopyObjectRequest.builder()
                    .sourceBucket(bucketName)
                    .sourceKey(sourceKey)
                    .destinationBucket(bucketName)
                    .destinationKey(targetKey)
                    .build();
            try {
                s3Client.copyObject(copyObjectRequest);
            } catch (S3Exception e) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "source case " + sourceCaseUuid + NOT_FOUND, e);
            }
        }
        CaseMetadataEntity existingCase = getCaseMetaDataEntity(sourceCaseUuid);
        createCaseMetadataEntity(newCaseUuid, withExpiration, existingCase.isIndexed(), existingCase.getOriginalFilename(), existingCase.getCompressionFormat(), existingCase.getFormat());
        CaseInfos existingCaseInfos = getCaseInfos(sourceCaseUuid);
        CaseInfos caseInfos = createInfos(existingCaseInfos.getName(), newCaseUuid, existingCaseInfos.getFormat());
        if (existingCase.isIndexed()) {
            caseInfosService.addCaseInfos(caseInfos);
        }
        notificationService.sendImportMessage(caseInfos.createMessage());
        return newCaseUuid;
    }

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

    public void setComputationManager(ComputationManager computationManager) {
        this.computationManager = Objects.requireNonNull(computationManager);
    }

}
