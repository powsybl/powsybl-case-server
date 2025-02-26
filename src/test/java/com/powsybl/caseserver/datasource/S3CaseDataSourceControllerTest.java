/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.service.MinioContainerConfig;
import com.powsybl.caseserver.service.S3CaseService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.UUID;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=S3"})
@ContextConfigurationWithTestChannel
class S3CaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest implements MinioContainerConfig {

    @Autowired
    protected S3CaseService s3CaseService;

    @BeforeEach
    void setUp() throws URISyntaxException, IOException {
        caseService = s3CaseService;

        //insert a cgmes in the FS
        cgmesCaseUuid = importCase(CGMES_ZIP_NAME, "application/zip");
        cgmesDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + CGMES_ZIP_NAME).toURI()));

        // insert plain file in the FS
        iidmCaseUuid = importCase(IIDM_FILE_NAME, "text/plain");
        iidmDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + IIDM_FILE_NAME).toURI()));

        // insert tar in the FS
        tarCaseUuid = importCase(IIDM_TAR_NAME, "application/tar");
        tarDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + IIDM_TAR_NAME).toURI()));

    }

    @Test
    void testExistsPlainWithExtraFolder() throws IOException {
        UUID caseUuid = importCase(IIDM_FILE_NAME, "text/plain");

        // Some implementations create an entry for the directory, mimic this behavior
        // minio doesn't do this so we do it manually
        S3Client s3client = s3CaseService.getS3Client();
        s3client.putObject(PutObjectRequest.builder()
                .bucket(s3CaseService.getBucketName())
                .key(s3CaseService.uuidToKeyPrefix(caseUuid))
                .build(), RequestBody.empty());

        assertTrue(s3CaseService.datasourceExists(caseUuid, IIDM_FILE_NAME), "datasourceExist should return true for a plain file even if there is a empty key for the directory");
    }
}
