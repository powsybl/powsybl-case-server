/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@TestPropertySource(properties = {"storage.type=FS"})
class FsCaseControllerTest extends AbstractCaseControllerTest {

    @Autowired
    private FsCaseService fsCaseService;

    @BeforeEach
    void setUp() {
        caseService = fsCaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ((FsCaseService) caseService).setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseMetadataRepository.deleteAll();
        outputDestination.clear();
    }

    @Test
    void testStorageNotCreated() throws Exception {
        // expect a fail since the storage dir. is not created
        mvc.perform(delete("/v1/cases")).andExpect(status().isUnprocessableEntity());
    }

    @Test
    void invalidFileInCaseDirectoryShouldBeIgnored() throws Exception {
        createStorageDir();

        // add a random file in the storage, not stored in a UUID named directory
        Path filePath = fileSystem.getPath(caseService.getRootDirectory()).resolve("randomFile.txt");
        Files.createFile(filePath);

        // import a case properly
        importCase(TEST_CASE, false);

        MvcResult mvcResult = mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andReturn();
        String resultAsString = mvcResult.getResponse().getContentAsString();
        List<CaseInfos> caseInfos = mapper.readValue(resultAsString, new TypeReference<>() { });
        assertEquals(1, caseInfos.size());
        assertEquals(TEST_CASE, caseInfos.get(0).getName());

        Files.delete(filePath);
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }

    @Override
    UUID addCaseWithoutMetadata() throws Exception {
        UUID caseUuid = UUID.randomUUID();
        Path casePath = fileSystem.getPath(caseService.getRootDirectory()).resolve(caseUuid.toString());
        Files.createDirectory(casePath);
        Files.write(casePath.resolve(TEST_CASE), AbstractCaseControllerTest.class.getResourceAsStream("/" + TEST_CASE).readAllBytes());
        return caseUuid;
    }
}
