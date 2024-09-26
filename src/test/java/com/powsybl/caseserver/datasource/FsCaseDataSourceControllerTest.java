/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.service.FsCaseService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.UUID;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=file"})
@ContextConfigurationWithTestChannel
public class FsCaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest {

    @Autowired
    protected FsCaseService caseService;

    FileSystem fileSystem = FileSystems.getDefault();

    @Before
    public void setUp() throws URISyntaxException, IOException {
        CASE_UUID = UUID.randomUUID();
        fileName = directoryPath + fileName;
        Path path = fileSystem.getPath(rootDirectory);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        Path caseDirectory = fileSystem.getPath(rootDirectory).resolve(CASE_UUID.toString());
        if (!Files.exists(caseDirectory)) {
            Files.createDirectories(caseDirectory);
        }

        caseService.setFileSystem(fileSystem);
        //insert a cgmes in the FS
        try (InputStream cgmesURL = getClass().getResourceAsStream("/" + cgmesName)) {
            Path cgmes = caseDirectory.resolve(cgmesName);
            Files.copy(cgmesURL, cgmes, StandardCopyOption.REPLACE_EXISTING);
        }
        dataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + cgmesName).toURI()));
    }
}
