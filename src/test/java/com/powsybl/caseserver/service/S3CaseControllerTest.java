/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@TestPropertySource(properties = {"storage.type=S3"})
class S3CaseControllerTest extends AbstractCaseControllerTest implements MinioContainerConfig {

    @Autowired
    private S3CaseService s3CaseService;

    @BeforeEach
    void setUp() {
        caseService = s3CaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseService.deleteAllCases();
        outputDestination.clear();
    }

    @Override
    UUID addCaseWithoutMetadata() throws Exception {
        UUID caseUuid = importCase(TEST_CASE, false);
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
        caseMetadataRepository.deleteById(caseUuid);
        return caseUuid;
    }

    @Override
    void addRandomFile() {
        RequestBody requestBody = RequestBody.fromString("");
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3CaseService.getBucketName())
                .key(s3CaseService.getRootDirectory() + "/randomFile.txt")
                .contentType("application/octet-stream")
                .build();
        s3CaseService.getS3Client().putObject(putObjectRequest, requestBody);
    }

    @Override
    void removeRandomFile() throws IOException {
        List<ObjectIdentifier> objectsToDelete = s3CaseService.getS3Client().listObjectsV2(builder -> builder.bucket(s3CaseService.getBucketName()).prefix(s3CaseService.getRootDirectory() + "/randomFile.txt"))
                .contents()
                .stream()
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .toList();
        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(s3CaseService.getBucketName())
                .delete(delete -> delete.objects(objectsToDelete))
                .build();
        s3CaseService.getS3Client().deleteObjects(deleteObjectsRequest);
    }
}
