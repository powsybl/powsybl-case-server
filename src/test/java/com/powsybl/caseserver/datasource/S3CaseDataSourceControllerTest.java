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
        caseService = s3CaseService;
        cgmesCaseUuid = importCase(CGMES_ZIP_NAME, "application/zip");
        cgmesDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + CGMES_ZIP_NAME).toURI()));
    }

}
