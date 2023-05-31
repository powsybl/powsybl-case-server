/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = {CaseInfosService.class})
//TODO make this an interface, with different implementations (filesystem or object storage)
public class CaseService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaseService.class);

    private static final String CATEGORY_BROKER_OUTPUT = CaseService.class.getName() + ".output-broker-messages";

    private static final Logger OUTPUT_MESSAGE_LOGGER = LoggerFactory.getLogger(CATEGORY_BROKER_OUTPUT);

    private FileSystem fileSystem = FileSystems.getDefault();

    private ComputationManager computationManager = LocalComputationManager.getDefault();

    private final CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private StreamBridge caseInfosPublisher;

    @Autowired
    private CaseInfosService caseInfosService;

    @Value("${case-store-directory:#{systemProperties['user.home'].concat(\"/cases\")}}")
    private String rootDirectory;

    @Value("${case-store-bucket}")
    private String bucketName;

    private static final String CASES_PREFIX = "gsi-cases/";

    @Autowired
    private AmazonS3Client s3Client;

    public CaseService(CaseMetadataRepository caseMetadataRepository) {
        this.caseMetadataRepository = caseMetadataRepository;
    }

    // TODO replace Path with InputStream but simple powsybl APIs need a Path to
    // make a datasource listing a zip content.
    // TODO use new multipledatasource ?
    Importer getImporterOrThrowsException(Path caseFile) {
        DataSource dataSource = DataSource.fromPath(caseFile);
        Importer importer = Importer.find(dataSource, computationManager);
        if (importer == null) {
            throw CaseException.createFileNotImportable(caseFile);
        }
        return importer;
    }

    String getFormat(Path caseFile) {
        Importer importer = getImporterOrThrowsException(caseFile);
        return importer.getFormat();
    }

    // creates a directory, and then in this directory, initializes a file with content.
    // After applying f to the file, deletes the file and the directory.
    private <R, T1 extends Throwable, T2 extends Throwable> R withTempCopy(UUID caseUuid, String filename,
            FailableConsumer<Path, T1> contentInitializer, FailableFunction<Path, R, T2> f) {
        Path tempdir;
        Path tempCasePath = null;
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
        String filename = parseFilenameFromKey(getCaseFileObjectKey(caseUuid));
        return withTempCopy(caseUuid, filename, path ->
            s3Client.getObject(new GetObjectRequest(bucketName, caseFileKey), path.toFile()),
        f);
    }

    // TODO don't download file to get the format, see getImporterOrThrowsException
    String getFormat(UUID caseUuid) {
        return withS3DownloadedTempPath(caseUuid, this::getFormat);
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
    private List<S3ObjectSummary> getCasesSummary(String prefix) {
        ObjectListing objectListing = s3Client.listObjects(bucketName, prefix);
        if (objectListing.isTruncated()) {
            LOGGER.warn("Truncated listing for prefix " + prefix); // TODO implement pagination
        }
        return objectListing.getObjectSummaries().stream()
                .filter(x -> !x.getKey().endsWith("/")).collect(Collectors.toList());
    }

    private S3ObjectSummary getCaseFileSummary(UUID caseUuid) {
        List<S3ObjectSummary> files = getCasesSummary(uuidToPrefixKey(caseUuid));
        if (files.size() > 1) {
            LOGGER.warn("Multiple files for case " + caseUuid); //TODO handle uuids that have multiple files
        }
        return files.stream().findFirst().orElseThrow();
    }

    public String getCaseFileObjectKey(UUID caseUuid) {
        S3ObjectSummary s3ObjectSummary = getCaseFileSummary(caseUuid);
        return s3ObjectSummary.getKey();
    }

    private CaseInfos infosFromDownloadCaseFileSummary(S3ObjectSummary objectSummary) {
        UUID uuid = parseUuidFromKey(objectSummary.getKey());
        return createInfos(parseFilenameFromKey(objectSummary.getKey()), uuid,
                getFormat(uuid) // TODO Store as metadata instead
        );
    }

    public CaseInfos getCase(UUID caseUuid) {
        return infosFromDownloadCaseFileSummary(getCaseFileSummary(caseUuid));
    }

    public String getCaseName(UUID caseUuid) {
        CaseInfos caseInfos = getCase(caseUuid);
        return caseInfos.getName();
    }

    public List<CaseInfos> getCases(String prefix) {
        return getCasesSummary(prefix).stream()
                .map(this::infosFromDownloadCaseFileSummary) // TODO handle uuids that have multiple files
            .collect(Collectors.toList());
    }

    public List<CaseInfos> getCases() {
        return getCases(CASES_PREFIX);
    }

    Optional<byte[]> getCaseBytes(UUID caseUuid) {
        String caseFileKey = getCaseFileObjectKey(caseUuid);
        S3Object s3object = s3Client.getObject(bucketName, caseFileKey);

        // TODO can this API really return null ?
        if (s3object == null) {
            return Optional.empty();
        }

        try {
            return Optional.of(s3object.getObjectContent().readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    boolean caseExists(UUID caseName) {
        //TODO what request does this do ?
        return !getCasesSummary(uuidToPrefixKey(caseName)).isEmpty();
    }

    UUID importCase(MultipartFile mpf, boolean withExpiration) {
        UUID caseUuid = UUID.randomUUID();

        String caseName = mpf.getOriginalFilename();
        validateCaseName(caseName);

        //TODO, remove this ? can't happend with a randomUUID
        if (caseExists(caseUuid)) {
            throw CaseException.createDirectoryAreadyExists(caseUuid.toString());
        }

        // TODO store this detected format to avoid having to recompute it later after
        // the case has been stored and clients do a request on the /format API
        String format = withTempCopy(caseUuid, caseName, mpf::transferTo, this::getFormat);

        try (InputStream inputStream = mpf.getInputStream()) {
            String key = uuidAndFilenameToKey(caseUuid, caseName);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(mpf.getContentType());
            metadata.setContentLength(mpf.getSize());
            s3Client.putObject(bucketName, key, inputStream, metadata);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        createCaseMetadataEntity(caseUuid, withExpiration);
        CaseInfos caseInfos = createInfos(mpf.getOriginalFilename(), caseUuid, format);
        caseInfosService.addCaseInfos(caseInfos);
        sendImportMessage(caseInfos.createMessage());

        return caseUuid;
    }

    UUID duplicateCase(UUID sourceCaseUuid, boolean withExpiration) {
        // TODO don't query twice, here in caseExist and for getCaseObjectKey
        if (!caseExists(sourceCaseUuid)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Source case " + sourceCaseUuid + " not found");
        }

        UUID newCaseUuid = UUID.randomUUID();

        String sourceKey = getCaseFileObjectKey(sourceCaseUuid);
        String targetKey = uuidAndFilenameToKey(newCaseUuid, parseFilenameFromKey(sourceKey));

        s3Client.copyObject(bucketName, sourceKey, bucketName, targetKey);

        CaseInfos existingCaseInfos = caseInfosService.getCaseInfosByUuid(sourceCaseUuid.toString()).orElseThrow();
        CaseInfos caseInfos = createInfos(existingCaseInfos.getName(), newCaseUuid, existingCaseInfos.getFormat());
        caseInfosService.addCaseInfos(caseInfos);
        createCaseMetadataEntity(newCaseUuid, withExpiration);

        sendImportMessage(caseInfos.createMessage());
        return newCaseUuid;
    }

    private void createCaseMetadataEntity(UUID newCaseUuid, boolean withExpiration) {
        LocalDateTime expirationTime = null;
        if (withExpiration) {
            expirationTime = LocalDateTime.now(ZoneOffset.UTC).plusHours(1);
        }
        caseMetadataRepository.save(new CaseMetadataEntity(newCaseUuid, expirationTime));
    }

    CaseInfos createInfos(String fileBaseName, UUID caseUuid, String format) {
        FileNameParser parser = FileNameParsers.findParser(fileBaseName);
        if (parser != null) {
            Optional<? extends FileNameInfos> fileNameInfos = parser.parse(fileBaseName);
            if (fileNameInfos.isPresent()) {
                return CaseInfos.create(fileBaseName, caseUuid, format, fileNameInfos.get());
            }
        }
        return CaseInfos.builder().name(fileBaseName).uuid(caseUuid).format(format).build();
    }

    @Transactional
    public void disableCaseExpiration(UUID caseUuid) {
        CaseMetadataEntity caseMetadataEntity = caseMetadataRepository.findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "case " + caseUuid + " not found"));
        caseMetadataEntity.setExpirationDate(null);
    }

    Optional<Network> loadNetwork(UUID caseUuid) {

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

    void deleteCase(UUID caseUuid) {
        List<KeyVersion> keys = s3Client.listObjects(bucketName, uuidToPrefixKey(caseUuid))
                .getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey).map(KeyVersion::new).collect(Collectors.toList());
        if (!keys.isEmpty()) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
            caseInfosService.deleteCaseInfosByUuid(caseUuid.toString());
            caseMetadataRepository.deleteById(caseUuid);
        }
    }

    void deleteAllCases() {
        List<KeyVersion> keys = s3Client.listObjects(bucketName, CASES_PREFIX)
                .getObjectSummaries().stream()
                .map(S3ObjectSummary::getKey).map(k -> new KeyVersion(k)).collect(Collectors.toList());

        if (!keys.isEmpty()) {
            s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keys));
        }
        caseInfosService.deleteAllCaseInfos();
        caseMetadataRepository.deleteAll();
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = Objects.requireNonNull(fileSystem);
    }

    public void setComputationManager(ComputationManager computationManager) {
        this.computationManager = Objects.requireNonNull(computationManager);
    }

    static void validateCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        if (!caseName.matches("[\\w\\-]+(\\.[\\w]+)*+")) {
            throw CaseException.createIllegalCaseName(caseName);
        }
    }

    /*
     The query is an elasticsearch (Lucene) form query, so here it will be :
     date:XXX AND geographicalCode:(X)
     date:XXX AND geographicalCode:(X OR Y OR Z)
    */
    List<CaseInfos> searchCases(String query) {
        return caseInfosService.searchCaseInfos(query);
    }

    private void sendImportMessage(Message<String> message) {
        OUTPUT_MESSAGE_LOGGER.debug("Sending message : {}", message);
        caseInfosPublisher.send("publishCaseImport-out-0", message);
    }

    public void reindexAllCases() {
        caseInfosService.recreateAllCaseInfos(getCases(CASES_PREFIX));
    }

    public List<CaseInfos> getMetadata(List<UUID> ids) {
        List<CaseInfos> cases = new ArrayList<>();
        ids.forEach(caseUuid -> {
            CaseInfos caseInfos = getCase(caseUuid);
            cases.add(caseInfos);
        });
        return cases;
    }
}
