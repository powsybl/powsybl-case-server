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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Paths;

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

        // insert tar in the FS
        tarCaseUuid = importCase(IIDM_TAR_NAME, "application/tar");
        tarDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + IIDM_TAR_NAME).toURI()));

        // insert plain file in the FS
        iidmCaseUuid = importCase(IIDM_FILE_NAME, "text/plain");
        iidmDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + IIDM_FILE_NAME).toURI()));
    }

}
