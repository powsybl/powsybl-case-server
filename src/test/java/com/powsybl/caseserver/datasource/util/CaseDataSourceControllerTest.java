/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.CaseApplication;
import com.powsybl.caseserver.CaseService;
import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.commons.datasource.DataSource;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextHierarchy;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@EnableWebMvc
@WebMvcTest(value = CaseDataSourceController.class, properties = {"case-store-directory=test"})
@ContextHierarchy({@ContextConfiguration(classes = {CaseApplication.class, TestChannelBinderConfiguration.class})})
class CaseDataSourceControllerTest {

    private FileSystem fileSystem = Jimfs.newFileSystem();

    @MockBean
    private StreamBridge streamBridge;

    @MockBean
    private EntityManager entityManager;

    @Autowired
    private MockMvc mvc;

    @MockBean
    private CaseMetadataRepository caseMetadataRepository;

    @MockBean
    private CaseInfosRepository caseInfosRepository;

    @Autowired
    private CaseService caseService;

    @Value("${case-store-directory:#{systemProperties['user.home'].concat(\"/cases\")}}")
    private String rootDirectory;

    private String cgmesName = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2.zip";
    private String fileName = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2/MicroGridTestConfiguration_BC_BE_DL_V2.xml";

    private static final UUID CASE_UUID = UUID.randomUUID();

    private DataSource dataSource;

    @Autowired
    private ObjectMapper mapper;

    @BeforeEach
    void setUp() throws Exception {
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

    @Test
    void testBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", CASE_UUID))
                .andExpect(status().isOk())
                .andReturn();

        assertEquals(dataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", CASE_UUID)
                .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        assertEquals(dataSource.listNames(".*"), nameList);
    }

    @Test
    void testInputStreamWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", CASE_UUID)
                .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        try (InputStreamReader isReader = new InputStreamReader(dataSource.newInputStream(fileName), StandardCharsets.UTF_8)) {
            BufferedReader reader = new BufferedReader(isReader);
            StringBuilder datasourceResponse = new StringBuilder();
            String str;
            while ((str = reader.readLine()) != null) {
                datasourceResponse.append(str).append("\n");
            }
            assertEquals(datasourceResponse.toString(), mvcResult.getResponse().getContentAsString());
        }
    }

    @Test
    void testInputStreamWithSuffixExt() throws Exception {
        String suffix = "/MicroGridTestConfiguration_BC_BE_DL_V2";
        String ext = "xml";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", CASE_UUID)
                .param("suffix", suffix)
                .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        try (InputStreamReader isReader = new InputStreamReader(dataSource.newInputStream(suffix, ext), StandardCharsets.UTF_8)) {
            BufferedReader reader = new BufferedReader(isReader);
            StringBuilder datasourceResponse = new StringBuilder();
            String str;
            while ((str = reader.readLine()) != null) {
                datasourceResponse.append(str).append("\n");
            }
            assertEquals(datasourceResponse.toString(), mvcResult.getResponse().getContentAsString());
        }
    }

    @Test
    void testExistsWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", CASE_UUID)
                .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists(fileName), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", CASE_UUID)
                .param("fileName", "random"))
                .andExpect(status().isOk())
                .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists("random"), res);
    }

    @Test
    void testExistsWithSuffixExt() throws Exception {
        String suffix = "random";
        String ext = "uct";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", CASE_UUID)
                .param("suffix", suffix)
                .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists(suffix, ext), res);
    }
}
