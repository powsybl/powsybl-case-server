/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.service.MinioContainerConfig;
import com.powsybl.caseserver.service.S3CaseService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=S3"})
@ContextConfigurationWithTestChannel
public class S3CaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest implements MinioContainerConfig {

    @Autowired
    protected S3CaseService s3CaseService;

    @Before
    public void setUp() throws URISyntaxException, IOException {
        System.out.println("TOTO");
        //insert a cgmes file in the S3
        try (InputStream cgmesURL = getClass().getResourceAsStream("/" + CGMES_ZIP_NAME);
             InputStream xiidmURL = getClass().getResourceAsStream("/" + XIIDM_ZIP_NAME)) {
            cgmesCaseUuid = s3CaseService.importCase(new MockMultipartFile(CGMES_ZIP_NAME, CGMES_ZIP_NAME, "application/zip", cgmesURL.readAllBytes()), false, false);
            xiidmCaseUuid = s3CaseService.importCase(new MockMultipartFile(XIIDM_ZIP_NAME, XIIDM_ZIP_NAME, "application/zip", xiidmURL.readAllBytes()), false, false);
        }
        cgmesDataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + CGMES_ZIP_NAME).toURI()));
        xiidmDataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + XIIDM_ZIP_NAME).toURI()));
    }
}
