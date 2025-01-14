/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.service.FsCaseService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.UUID;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=FS"})
@ContextConfigurationWithTestChannel
class FsCaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest {

    @Autowired
    protected FsCaseService fsCaseService;

    FileSystem fileSystem = Jimfs.newFileSystem();

    @BeforeEach
    void setUp() throws URISyntaxException, IOException {
        caseService = fsCaseService;
        cgmesCaseUuid = UUID.randomUUID();
        Path path = fileSystem.getPath(caseService.getRootDirectory());
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Path cgmesCaseDirectory = fileSystem.getPath(caseService.getRootDirectory()).resolve(cgmesCaseUuid.toString());
        if (!Files.exists(cgmesCaseDirectory)) {
            Files.createDirectories(cgmesCaseDirectory);
        }

        fsCaseService.setFileSystem(fileSystem);
        //insert a cgmes in the FS
        try (InputStream cgmesURL = getClass().getResourceAsStream("/" + CGMES_ZIP_NAME);
        ) {
            Files.copy(cgmesURL, cgmesCaseDirectory.resolve(CGMES_ZIP_NAME), StandardCopyOption.REPLACE_EXISTING);
        }
        cgmesDataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + CGMES_ZIP_NAME).toURI()));

        iidmCaseUuid = importIidmCase();
        iidmDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + IIDM_NAME).toURI()));
    }
}
