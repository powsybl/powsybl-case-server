/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.caseserver.services.SupervisionService;
import com.powsybl.computation.ComputationManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {"case-store-directory=/cases"})
@ContextConfigurationWithTestChannel
public class SupervisionControllerTest {
    @Autowired
    SupervisionService supervisionService;
    @Autowired
    CaseInfosRepository caseInfosRepository;
    @Autowired
    CaseMetadataRepository caseMetadataRepository;
    @Autowired
    CaseService caseService;

    @Autowired
    private MockMvc mockMvc;

    @Value("${case-store-directory}")
    private String rootDirectory;

    private static final String TEST_CASE = "testCase.xiidm";
    private FileSystem fileSystem;

    @Test
    public void testGetElementInfosCount() throws Exception {
        createStorageDir();
        importCase(true);
        importCase(true);
        importCase(false);

        mockMvc.perform(post("/v1/supervision/cases/reindex"))
                .andExpect(status().isOk());

        Assert.assertEquals(2, supervisionService.getIndexedCaseElementsCount());

    }

    @Test
    public void testDeleteElementInfos() throws Exception {
        createStorageDir();
        importCase(true);
        importCase(true);

        mockMvc.perform(delete("/v1/supervision/cases/indexation"))
                .andExpect(status().isOk());
        Assert.assertEquals(0, supervisionService.getIndexedCaseElementsCount());

        //reindex
        mockMvc.perform(post("/v1/supervision/cases/reindex"))
                .andExpect(status().isOk());

        Assert.assertEquals(2, supervisionService.getIndexedCaseElementsCount());
    }

    private void importCase(Boolean indexed) throws Exception {
        mockMvc.perform(multipart("/v1/cases")
                            .file(createMockMultipartFile())
                            .param("indexed", indexed.toString()))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
    }

    private static MockMultipartFile createMockMultipartFile() throws IOException {
        try (InputStream inputStream = CaseControllerTest.class.getResourceAsStream("/" + SupervisionControllerTest.TEST_CASE)) {
            return new MockMultipartFile("file", SupervisionControllerTest.TEST_CASE, MediaType.TEXT_PLAIN_VALUE, inputStream);
        }
    }

    @Before
    public void setUp() {
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        caseService.setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        cleanDB();
    }

    @After
    public void tearDown() throws Exception {
        fileSystem.close();
    }

    private void cleanDB() {
        caseMetadataRepository.deleteAll();
    }

    private void createStorageDir() throws IOException {
        Path path = fileSystem.getPath(rootDirectory);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }
}
