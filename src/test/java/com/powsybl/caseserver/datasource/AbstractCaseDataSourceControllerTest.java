/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import com.powsybl.caseserver.service.CaseService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@DisableElasticsearch
public abstract class AbstractCaseDataSourceControllerTest {

    @MockBean
    StreamBridge streamBridge;

    @Autowired
    private MockMvc mvc;

    protected static CaseService caseService;

    static final String CGMES_ZIP_NAME = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2.zip";

    static final String CGMES_FILE_NAME = "CGMES_v2415_MicroGridTestConfiguration_BC_BE_v2/MicroGridTestConfiguration_BC_BE_DL_V2.xml";

    static final String IIDM_TAR_NAME = "tarCase.tar";

    static final String PLAIN_IIDM_TAR_NAME = "tarCase.xiidm";

    static final String IIDM_FILE_NAME = "testCase.xiidm";

    UUID cgmesCaseUuid;

    UUID tarCaseUuid;

    protected DataSource cgmesDataSource;

    protected DataSource tarDataSource;

    private final ObjectMapper mapper = new ObjectMapper();

    UUID iidmCaseUuid;

    protected DataSource iidmDataSource;

    protected static UUID importCase(String filename, String contentType) throws IOException {
        UUID caseUUID;
        try (InputStream inputStream = S3CaseDataSourceControllerTest.class.getResourceAsStream("/" + filename)) {
            caseUUID = caseService.importCase(new MockMultipartFile(filename, filename, contentType, inputStream.readAllBytes()), false, false);
        }
        return caseUUID;
    }

    @Test
    public void testBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", cgmesCaseUuid))
                .andExpect(status().isOk())
                .andReturn();

        assertEquals(cgmesDataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", cgmesCaseUuid)
                        .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        assertEquals(cgmesDataSource.listNames(".*"), nameList);
    }

    @Test
    public void testInputStreamWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", cgmesCaseUuid)
                        .param("fileName", CGMES_FILE_NAME))
                .andExpect(status().isOk())
                .andReturn();

        try (InputStreamReader isReader = new InputStreamReader(cgmesDataSource.newInputStream(CGMES_FILE_NAME), StandardCharsets.UTF_8)) {
            BufferedReader reader = new BufferedReader(isReader);
            StringBuilder datasourceResponse = new StringBuilder();
            String str;
            while ((str = reader.readLine()) != null) {
                datasourceResponse.append(str).append("\n");
            }
            assertEquals(datasourceResponse.toString(), mvcResult.getResponse().getContentAsString());
        }
    }

    private static String readDataSource(DataSource dataSource, String fileName) throws Exception {
        try (InputStreamReader isReader = new InputStreamReader(dataSource.newInputStream(fileName), StandardCharsets.UTF_8)) {
            BufferedReader reader = new BufferedReader(isReader);
            StringBuilder datasourceResponse = new StringBuilder();
            String str;
            while ((str = reader.readLine()) != null) {
                datasourceResponse.append(str).append("\n");
            }
            return datasourceResponse.toString();
        }
    }

    @Test
    public void testInputStreamWithZipFile() throws Exception {
        String zipName = "LF.zip";
        String fileName = "LF.xml";
        UUID caseUuid = importCase(zipName, "application/zip");
        DataSource dataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + zipName).toURI()));

        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
                .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();
        assertEquals(readDataSource(dataSource, fileName), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testInputStreamWithGZipFile() throws Exception {
        String gzipName = "LF.xml.gz";
        String fileName = "LF.xml";
        UUID caseUuid = importCase(gzipName, "application/zip");
        DataSource dataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + gzipName).toURI()));

        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();
        assertEquals(readDataSource(dataSource, fileName), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testInputStreamWithXiidmPlainFile() throws Exception {
        String fileName = "LF.xml";
        UUID caseUuid = importCase(fileName, "application/zip");
        DataSource dataSource = DataSource.fromPath(Paths.get(S3CaseDataSourceControllerTest.class.getResource("/" + fileName).toURI()));

        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        assertEquals(readDataSource(dataSource, fileName), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testInputStreamWithSuffixExt() throws Exception {
        String suffix = "/MicroGridTestConfiguration_BC_BE_DL_V2";
        String ext = "xml";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", cgmesCaseUuid)
                        .param("suffix", suffix)
                        .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        try (InputStreamReader isReader = new InputStreamReader(cgmesDataSource.newInputStream(suffix, ext), StandardCharsets.UTF_8)) {
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
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", cgmesCaseUuid)
                        .param("fileName", CGMES_FILE_NAME))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(cgmesDataSource.exists(CGMES_FILE_NAME), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", cgmesCaseUuid)
                        .param("fileName", "random"))
                .andExpect(status().isOk())
                .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(cgmesDataSource.exists("random"), res);
    }

    @Test
    public void testExistsWithSuffixExt() throws Exception {
        String suffix = "random";
        String ext = "uct";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", cgmesCaseUuid)
                        .param("suffix", suffix)
                        .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(cgmesDataSource.exists(suffix, ext), res);
    }

    @Test
    void testBaseNameWithIidm() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", iidmCaseUuid))
            .andExpect(status().isOk())
            .andReturn();

        Assertions.assertEquals(iidmDataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testListNameWithIidm() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", iidmCaseUuid)
                .param("regex", ".*"))
            .andExpect(status().isOk())
            .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        Assertions.assertEquals(iidmDataSource.listNames(".*"), nameList);
    }

    @Test
    void testExistsWithFileNameWithIidm() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", iidmCaseUuid)
                .param("fileName", IIDM_FILE_NAME))
            .andExpect(status().isOk())
            .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(iidmDataSource.exists(IIDM_FILE_NAME), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", iidmCaseUuid)
                .param("fileName", "random"))
            .andExpect(status().isOk())
            .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(iidmDataSource.exists("random"), res);
    }

    @Test
    void testExistsWithSuffixExtWithIidm() throws Exception {
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

    // tar tests
    @Test
    public void testTarBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", tarCaseUuid))
                .andExpect(status().isOk())
                .andReturn();

        assertEquals(tarDataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    public void testTarListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", tarCaseUuid)
                        .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        assertEquals(tarDataSource.listNames(".*"), nameList);
    }

    @Test
    public void testTarInputStreamWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", tarCaseUuid)
                        .param("fileName", PLAIN_IIDM_TAR_NAME))
                .andExpect(status().isOk())
                .andReturn();

        try (InputStreamReader isReader = new InputStreamReader(tarDataSource.newInputStream(PLAIN_IIDM_TAR_NAME), StandardCharsets.UTF_8)) {
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
    public void testTarExistsWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", tarCaseUuid)
                        .param("fileName", IIDM_TAR_NAME))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(tarDataSource.exists(IIDM_TAR_NAME), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", tarCaseUuid)
                        .param("fileName", "random"))
                .andExpect(status().isOk())
                .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(tarDataSource.exists("random"), res);
    }

    @Test
    public void testTarExistsWithSuffixExt() throws Exception {
        String suffix = "random";
        String ext = "uct";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", tarCaseUuid)
                        .param("suffix", suffix)
                        .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        assertEquals(tarDataSource.exists(suffix, ext), res);
    }

}
