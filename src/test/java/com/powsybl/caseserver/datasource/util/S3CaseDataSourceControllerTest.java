/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource.util;

import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.commons.datasource.DataSource;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=S3"})
@ContextConfigurationWithTestChannel
public class S3CaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest {

    @Autowired
    private S3Client s3Client;

    private static final String CASES_PREFIX = "gsi-cases/";

    @Before
    public void setUp() throws URISyntaxException, IOException {

        // TODO : mock S3CaseDataSourceService methods

        final var key = CASES_PREFIX + CASE_UUID + "/" + cgmesName;

        //insert a cgmes file in the S3
        try (InputStream cgmesURL = getClass().getResourceAsStream("/" + cgmesName)) {

            Map<String, String> userMetadata = new HashMap<>();
            userMetadata.put("format", "CGMES");
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(key)
                    .contentType("application/octet-stream")
                    .metadata(userMetadata)
                    .build();
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(cgmesURL.readAllBytes()));
        }

        dataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + cgmesName).toURI()));

    }
}
