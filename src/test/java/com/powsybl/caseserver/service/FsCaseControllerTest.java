/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.CaseException;
import com.powsybl.caseserver.utils.TestUtils;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@TestPropertySource(properties = {"storage.type=FS"})
class FsCaseControllerTest extends AbstractCaseControllerTest {

    FileSystem fileSystem;

    @Autowired
    private FsCaseService fsCaseService;

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
        List<String> destinations = List.of(caseImportDestination);
        TestUtils.assertQueuesEmptyThenClear(destinations, outputDestination);
    }

    void createStorageDir() throws IOException {
        Path path = fileSystem.getPath(caseService.getRootDirectory());
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }

    void deleteStorageDir() throws IOException {
        Path path = fileSystem.getPath(caseService.getRootDirectory());
        if (Files.exists(path)) {
            Files.delete(path);
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        caseService = fsCaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ((FsCaseService) caseService).setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseMetadataRepository.deleteAll();
        outputDestination.clear();
        createStorageDir();
    }

    @Test
    void testStorageNotCreated() throws Exception {
        deleteStorageDir();
        // expect a fail since the storage dir. is not created
        CaseException exception = assertThrows(CaseException.class, () -> caseService.deleteAllCases());
        assertEquals(exception.getMessage(), "The storage is not initialized: " + fsCaseService.getRootDirectory());
    }

    @Override
    void addRandomFile() throws IOException {
        Files.createFile(fileSystem.getPath(caseService.getRootDirectory()).resolve("randomFile.txt"));
    }

    @Override
    void removeFile(String caseName) throws IOException {
        FileSystemUtils.deleteRecursively(fileSystem.getPath(caseService.getRootDirectory()).resolve(caseName));
    }
}
