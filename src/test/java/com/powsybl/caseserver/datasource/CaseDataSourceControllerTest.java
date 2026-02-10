/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.service.MinioContainerConfig;
import com.powsybl.commons.datasource.DataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@DisableElasticsearch
@Import(DisableElasticsearch.MockConfig.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@ContextConfigurationWithTestChannel
class CaseDataSourceControllerTest implements MinioContainerConfig {

    @MockitoBean
    StreamBridge streamBridge;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private CaseService caseService;

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

    protected UUID importCase(String filename, String contentType) throws IOException {
        UUID caseUuid = UUID.randomUUID();
        try (InputStream inputStream = CaseDataSourceControllerTest.class.getResourceAsStream("/" + filename)) {
            caseService.importCase(new MockMultipartFile(filename, filename, contentType, inputStream.readAllBytes()), false, false, caseUuid);
        }
        return caseUuid;
    }

    @BeforeEach
    void setUp() throws URISyntaxException, IOException {

        //insert a cgmes in the FS
        cgmesCaseUuid = importCase(CGMES_ZIP_NAME, "application/zip");
        cgmesDataSource = DataSource.fromPath(Paths.get(CaseDataSourceControllerTest.class.getResource("/" + CGMES_ZIP_NAME).toURI()));

        // insert plain file in the FS
        iidmCaseUuid = importCase(IIDM_FILE_NAME, "text/plain");
        iidmDataSource = DataSource.fromPath(Paths.get(CaseDataSourceControllerTest.class.getResource("/" + IIDM_FILE_NAME).toURI()));

        // insert tar in the FS
        tarCaseUuid = importCase(IIDM_TAR_NAME, "application/tar");
        tarDataSource = DataSource.fromPath(Paths.get(CaseDataSourceControllerTest.class.getResource("/" + IIDM_TAR_NAME).toURI()));
    }

    @Test
    void testExistsPlainWithExtraFolder() throws IOException {
        UUID caseUuid = importCase(IIDM_FILE_NAME, "text/plain");

        // Some implementations create an entry for the directory, mimic this behavior
        // minio doesn't do this so we do it manually
        S3Client s3client = caseService.getS3Client();
        s3client.putObject(PutObjectRequest.builder()
                .bucket(caseService.getBucketName())
                .key(caseService.uuidToKeyPrefix(caseUuid))
                .build(), RequestBody.empty());

        assertTrue(caseService.datasourceExists(caseUuid, IIDM_FILE_NAME), "datasourceExist should return true for a plain file even if there is a empty key for the directory");
    }

    @Test
    void testBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", cgmesCaseUuid))
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(cgmesDataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", cgmesCaseUuid)
                        .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        Assertions.assertEquals(cgmesDataSource.listNames(".*"), nameList);
    }

    @Test
    void testInputStreamWithFileName() throws Exception {
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
            Assertions.assertEquals(datasourceResponse.toString(), mvcResult.getResponse().getContentAsString());
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
    void testInputStreamWithZipFile() throws Exception {
        String zipName = "LF.zip";
        String fileName = "LF.xml";
        UUID caseUuid = importCase(zipName, "application/zip");
        DataSource dataSource = DataSource.fromPath(Paths.get(CaseDataSourceControllerTest.class.getResource("/" + zipName).toURI()));

        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
                .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();
        Assertions.assertEquals(readDataSource(dataSource, fileName), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testInputStreamWithGZipFile() throws Exception {
        String gzipName = "LF.xml.gz";
        String fileName = "LF.xml";
        UUID caseUuid = importCase(gzipName, "application/zip");
        DataSource dataSource = DataSource.fromPath(Paths.get(CaseDataSourceControllerTest.class.getResource("/" + gzipName).toURI()));

        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();
        Assertions.assertEquals(readDataSource(dataSource, fileName), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testInputStreamWithXiidmPlainFile() throws Exception {
        String fileName = "LF.xml";
        UUID caseUuid = importCase(fileName, "application/zip");
        DataSource dataSource = DataSource.fromPath(Paths.get(CaseDataSourceControllerTest.class.getResource("/" + fileName).toURI()));

        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource", caseUuid)
                        .param("fileName", fileName))
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(readDataSource(dataSource, fileName), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testInputStreamWithSuffixExt() throws Exception {
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
            Assertions.assertEquals(datasourceResponse.toString(), mvcResult.getResponse().getContentAsString());
        }
    }

    @Test
    void testExistsWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", cgmesCaseUuid)
                        .param("fileName", CGMES_FILE_NAME))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(cgmesDataSource.exists(CGMES_FILE_NAME), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", cgmesCaseUuid)
                        .param("fileName", "random"))
                .andExpect(status().isOk())
                .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(cgmesDataSource.exists("random"), res);
    }

    @Test
    void testExistsWithSuffixExt() throws Exception {
        String suffix = "random";
        String ext = "uct";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", cgmesCaseUuid)
                        .param("suffix", suffix)
                        .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(cgmesDataSource.exists(suffix, ext), res);
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
    void testTarBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/baseName", tarCaseUuid))
                .andExpect(status().isOk())
                .andReturn();

        Assertions.assertEquals(tarDataSource.getBaseName(), mvcResult.getResponse().getContentAsString());
    }

    @Test
    void testTarListName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/list", tarCaseUuid)
                        .param("regex", ".*"))
                .andExpect(status().isOk())
                .andReturn();

        Set<String> nameList = mapper.readValue(mvcResult.getResponse().getContentAsString(), Set.class);
        Assertions.assertEquals(tarDataSource.listNames(".*"), nameList);
    }

    @Test
    void testTarInputStreamWithFileName() throws Exception {
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
            Assertions.assertEquals(datasourceResponse.toString(), mvcResult.getResponse().getContentAsString());
        }
    }

    @Test
    void testTarExistsWithFileName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", tarCaseUuid)
                        .param("fileName", IIDM_TAR_NAME))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(tarDataSource.exists(IIDM_TAR_NAME), res);

        mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", tarCaseUuid)
                        .param("fileName", "random"))
                .andExpect(status().isOk())
                .andReturn();

        res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(tarDataSource.exists("random"), res);
    }

    @Test
    void testTarExistsWithSuffixExt() throws Exception {
        String suffix = "random";
        String ext = "uct";
        MvcResult mvcResult = mvc.perform(get("/v1/cases/{caseUuid}/datasource/exists", tarCaseUuid)
                        .param("suffix", suffix)
                        .param("ext", ext))
                .andExpect(status().isOk())
                .andReturn();

        Boolean res = mapper.readValue(mvcResult.getResponse().getContentAsString(), Boolean.class);
        Assertions.assertEquals(tarDataSource.exists(suffix, ext), res);
    }

}
