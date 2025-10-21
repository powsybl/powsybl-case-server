/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.service.FsCaseService;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.EnableTestBinder;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=FS"})
@EnableTestBinder
class FsSupervisionControllerTest extends AbstractSupervisionControllerTest {

    FileSystem fileSystem;

    @Autowired
    private FsCaseService fsCaseService;

    @BeforeEach
    void setUp() throws IOException {
        caseService = fsCaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ((FsCaseService) caseService).setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        createStorageDir();
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
        caseMetadataRepository.deleteAll();
    }

    private void createStorageDir() throws IOException {
        Path path = fileSystem.getPath(caseService.getRootDirectory());
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }
}
