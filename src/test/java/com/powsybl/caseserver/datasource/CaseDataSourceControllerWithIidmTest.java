/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.service.S3CaseService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
@DisableElasticsearch
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=S3"})
@ContextConfigurationWithTestChannel
class CaseDataSourceControllerWithIidmTest {

    @MockBean
    StreamBridge streamBridge;

    @Autowired
    private MockMvc mvc;

    @Autowired
    protected S3CaseService s3CaseService;

    protected static CaseService caseService;

    static final String IIDM_NAME = "testCase.xiidm";

    UUID iidmCaseUuid;

    protected DataSource iidmDataSource;

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws URISyntaxException, IOException {
        caseService = s3CaseService;
        iidmCaseUuid = importCase();
        iidmDataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + IIDM_NAME).toURI()));
    }

    private static UUID importCase() throws IOException {
        UUID caseUUID;
        try (InputStream inputStream = S3CaseDataSourceControllerTest.class.getResourceAsStream("/" + CaseDataSourceControllerWithIidmTest.IIDM_NAME)) {
            caseUUID = caseService.importCase(new MockMultipartFile(CaseDataSourceControllerWithIidmTest.IIDM_NAME, CaseDataSourceControllerWithIidmTest.IIDM_NAME, "application/zip", inputStream.readAllBytes()), false, false);
        }
        return caseUUID;
    }

    @Test
    void testBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", iidmCaseUuid))
            .andExpect(status().isOk())
            .andReturn();

        Assertions.assertEquals(iidmDataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", iidmCaseUuid)
                .param("regex", ".*"))
            .andExpect(status().isOk())
            .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        Assertions.assertEquals(iidmDataSource.listNames(".*"), nameList);
    }

    @Test
    void testExistsWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", iidmCaseUuid)
                .param("fileName", IIDM_NAME))
            .andExpect(status().isOk())
            .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(iidmDataSource.exists(IIDM_NAME), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", iidmCaseUuid)
                .param("fileName", "random"))
            .andExpect(status().isOk())
            .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(iidmDataSource.exists("random"), res);
    }

    @Test
    void testExistsWithSuffixExt() throws Exception {
        String suffix = "random";
        String ext = "uct";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", iidmCaseUuid)
                .param("suffix", suffix)
                .param("ext", ext))
            .andExpect(status().isOk())
            .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(iidmDataSource.exists(suffix, ext), res);
    }
}
