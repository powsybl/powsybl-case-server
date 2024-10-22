/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.caseserver.services.SupervisionService;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@AutoConfigureMockMvc
@SpringBootTest(classes = {CaseApplication.class}, properties = {"case-store-directory=/cases"})
class SupervisionControllerTest {
    @Autowired
    private SupervisionService supervisionService;
    @Autowired
    private CaseMetadataRepository caseMetadataRepository;
    @Autowired
    private CaseService caseService;

    @Autowired
    private MockMvc mockMvc;

    @Value("${case-store-directory}")
    private String rootDirectory;

    private static final String TEST_CASE = "testCase.xiidm";
    private FileSystem fileSystem;

    @Test
    void testGetCaseInfosCount() throws Exception {
        createStorageDir();
        importCase(true);
        importCase(true);
        importCase(false);
        mockMvc.perform(post("/v1/supervision/cases/reindex"))
                .andExpect(status().isOk());
        assertEquals(2, supervisionService.getIndexedCasesCount());
    }

    @Test
    void testReindexAll() throws Exception {
        createStorageDir();
        importCase(true);
        importCase(true);
        importCase(false);

        mockMvc.perform(delete("/v1/supervision/cases/indexation"))
                .andExpect(status().isOk());

        assertEquals(0, supervisionService.getIndexedCasesCount());

        //reindex
        mockMvc.perform(post("/v1/supervision/cases/reindex"))
                .andExpect(status().isOk());

        String countStr = mockMvc.perform(get("/v1/supervision/cases/indexation-count"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertEquals("2", countStr);
        assertEquals(2, supervisionService.getIndexedCasesCount());

    }

    @Test
    void testGetIndexName() throws Exception {
        String result = mockMvc.perform(get("/v1/supervision/cases/index-name"))
                        .andReturn().getResponse().getContentAsString();
        assertEquals("cases", result);
    }

    private void importCase(Boolean indexed) throws Exception {
        mockMvc.perform(multipart("/v1/cases")
                            .file(createMockMultipartFile())
                            .param("withIndexation", indexed.toString()))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
    }

    private static MockMultipartFile createMockMultipartFile() throws IOException {
        try (InputStream inputStream = CaseControllerTest.class.getResourceAsStream("/" + SupervisionControllerTest.TEST_CASE)) {
            return new MockMultipartFile("file", SupervisionControllerTest.TEST_CASE, MediaType.TEXT_PLAIN_VALUE, inputStream);
        }
    }

    @BeforeEach
    void setUp() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        caseService.setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
    }

    @AfterEach
    void tearDown() throws Exception {
        fileSystem.close();
        caseMetadataRepository.deleteAll();
    }

    private void createStorageDir() throws IOException {
        Path path = fileSystem.getPath(rootDirectory);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }
}
