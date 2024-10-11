/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
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

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */

@DisableElasticsearch
public abstract class AbstractCaseDataSourceControllerTest {

    @MockBean
    StreamBridge streamBridge;

    @Autowired
    private MockMvc mvc;

    @Value("${case-store-directory:#{systemProperties['user.home'].concat(\"/cases\")}}")
    protected String rootDirectory;

    String cgmesName = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2.zip";

    String fileName = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2/MicroGridTestConfiguration_BC_BE_DL_V2.xml";

    UUID caseUuid;

    protected DataSource dataSource;

    private ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", caseUuid))
                .andExpect(status().isOk())
                .andReturn();

        assertEquals(dataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", caseUuid)
                        .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        assertEquals(dataSource.listNames(".*"), nameList);
    }

    @Test
    public void testInputStreamWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
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
    public void testInputStreamWithSuffixExt() throws Exception {
        String suffix = "/MicroGridTestConfiguration_BC_BE_DL_V2";
        String ext = "xml";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
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
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", caseUuid)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists(fileName), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", caseUuid)
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
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", caseUuid)
                        .param("suffix", suffix)
                        .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(dataSource.exists(suffix, ext), res);
    }

}
