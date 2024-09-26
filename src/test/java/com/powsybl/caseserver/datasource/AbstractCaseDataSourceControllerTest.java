/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.caseserver.service.AbstractContainerConfig;
import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.commons.datasource.DataSource;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */

public abstract class AbstractCaseDataSourceControllerTest extends AbstractContainerConfig {

    @MockBean
    StreamBridge streamBridge;

    @Autowired
    private MockMvc mvc;

    @MockBean
    CaseMetadataRepository caseMetadataRepository;

    @MockBean
    CaseInfosRepository caseInfosRepository;

    @Value("${case-store-directory:#{systemProperties['user.home'].concat(\"/cases\")}}")
    protected String rootDirectory;

    String cgmesName = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2.zip";

    String fileName = "MicroGridTestConfiguration_BC_BE_DL_V2.xml";

    String directoryPath = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2/";

    String prefix = "";

    static UUID CASE_UUID;

    protected DataSource dataSource;

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", CASE_UUID))
                .andExpect(status().isOk())
                .andReturn();

        assertEquals(dataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", CASE_UUID)
                        .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        nameList = nameList.stream().map(n -> prefix + n).collect(Collectors.toSet());
        assertEquals(dataSource.listNames(".*"), nameList);
    }

    @Test
    public void testInputStreamWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", CASE_UUID)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        try (InputStreamReader isReader = new InputStreamReader(dataSource.newInputStream(prefix + fileName), StandardCharsets.UTF_8)) {
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
    public void testInputStreamWithSuffixExt() throws Exception {
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
    public void testExistsWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", CASE_UUID)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists(prefix + fileName), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", CASE_UUID)
                        .param("fileName", "random"))
                .andExpect(status().isOk())
                .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists("random"), res);
    }

    @Test
    public void testExistsWithSuffixExt() throws Exception {
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
