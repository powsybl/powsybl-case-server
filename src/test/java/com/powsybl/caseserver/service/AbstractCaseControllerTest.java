/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.parsers.entsoe.EntsoeFileNameParser;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.caseserver.utils.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.test.OutputDestination;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@ContextConfigurationWithTestChannel
abstract class AbstractCaseControllerTest {
    static final String TEST_CASE = "testCase.xiidm";
    private static final String TEST_TAR_CASE = "tarCase.tar";
    private static final String TEST_CASE_FORMAT = "XIIDM";
    private static final String NOT_A_NETWORK = "notANetwork.txt";
    private static final String STILL_NOT_A_NETWORK = "stillNotANetwork.xiidm";
    private static final String GET_CASE_URL = "/v1/cases/{caseUuid}";
    private static final String GET_CASE_FORMAT_URL = "/v1/cases/{caseName}/format";

    private static final UUID RANDOM_UUID = UUID.fromString("3e2b6777-fea5-4e76-9b6b-b68f151373ab");
    private static final UUID CASE_UUID_TO_IMPORT = UUID.fromString("88601ab1-530e-47d2-8881-0dedecd6e6ee");

    @Autowired
    protected MockMvc mvc;

    CaseService caseService;

    @Autowired
    CaseMetadataRepository caseMetadataRepository;

    @Autowired
    OutputDestination outputDestination;

    @Autowired
    ObjectMapper mapper;

    FileSystem fileSystem;

    final String caseImportDestination = "case.import.destination";

    @AfterEach
    public void tearDown() throws Exception {
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

    private static MockMultipartFile createMockMultipartFile(String fileName) throws IOException {
        try (InputStream inputStream = AbstractCaseControllerTest.class.getResourceAsStream("/" + fileName)) {
            return new MockMultipartFile("file", fileName, MediaType.TEXT_PLAIN_VALUE, inputStream);
        }
    }

    @Test
    void testDeleteCases() throws Exception {
        // create the storage dir
        createStorageDir();

        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());
    }

    @Test
    void testCheckNonExistingCase() throws Exception {
        // create the storage dir
        createStorageDir();

        // check if the case exists (except a false)
        mvc.perform(get("/v1/cases/{caseUuid}/exists", RANDOM_UUID))
                .andExpect(status().isOk())
                .andExpect(content().string("false"))
                .andReturn();
    }

    @Test
    void testImportValidCase() throws Exception {
        createStorageDir();

        // import a case
        UUID firstCaseUuid = importCase(TEST_CASE, false);

        // assert that the broker message has been sent
        Message<byte[]> messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        MessageHeaders headersCase = messageImport.getHeaders();
        assertEquals(TEST_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(firstCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        //check that the case doesn't have an expiration date
        CaseMetadataEntity caseMetadataEntity = caseMetadataRepository.findById(firstCaseUuid).orElseThrow();
        assertEquals(firstCaseUuid, caseMetadataEntity.getId());
        assertNull(caseMetadataEntity.getExpirationDate());

        // retrieve case format
        mvc.perform(get(GET_CASE_FORMAT_URL, firstCaseUuid))
                .andExpect(status().isOk())
                .andExpect(content().string(TEST_CASE_FORMAT))
                .andReturn();

        //retrieve case name
        mvc.perform(get("/v1/cases/{caseUuid}/name", firstCaseUuid))
                .andExpect(status().isOk())
                .andExpect(content().string(TEST_CASE))
                .andReturn();

        //retrieve unknown case name
        mvc.perform(get("/v1/cases/{caseUuid}/name", UUID.randomUUID()))
                .andExpect(status().is5xxServerError());

        //retrieve case infos
        mvc.perform(get("/v1/cases/{caseUuid}/infos", firstCaseUuid))
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$.name").value(TEST_CASE))
                .andExpect(MockMvcResultMatchers.jsonPath("$.format").value(TEST_CASE_FORMAT))
                .andReturn();

        //retrieve case infos
        mvc.perform(get("/v1/cases/{caseUuid}/infos", UUID.randomUUID()))
                .andExpect(status().isNoContent())
                .andReturn();

        // check if the case exists (except a true)
        mvc.perform(get("/v1/cases/{caseUuid}/exists", firstCaseUuid))
                .andExpect(status().isOk())
                .andExpect(content().string("true"))
                .andReturn();
    }

    @Test
    void testImportInvalidFile() throws Exception {
        createStorageDir();

        // import a non valid case and expect a fail
        mvc.perform(multipart("/v1/cases")
                        .file(createMockMultipartFile(NOT_A_NETWORK)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(startsWith("This file cannot be imported")))
                .andReturn();

        // import a non valid case with a valid extension and expect a fail
        mvc.perform(multipart("/v1/cases")
                        .file(createMockMultipartFile(STILL_NOT_A_NETWORK)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(startsWith("This file cannot be imported")))
                .andReturn();
    }

    @Test
    void testDownloadNonExistingCase() throws Exception {
        createStorageDir();

        // download a non existing case
        mvc.perform(get(GET_CASE_URL, UUID.randomUUID()))
                .andExpect(status().isNoContent())
                .andReturn();
    }

    @Test
    void testExportNonExistingCaseFromat() throws Exception {
        createStorageDir();

        // import a case
        UUID firstCaseUuid = importCase(TEST_CASE, false);

        // export a case in a non-existing format
        mvc.perform(post(GET_CASE_URL, firstCaseUuid).param("format", "JPEG"))
                .andExpect(status().isUnprocessableEntity());
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }

    @Test
    void deleteNonExistingCase() throws Exception {
        createStorageDir();

        // import a case
        UUID caseaseUuid = importCase(TEST_CASE, false);
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

        // delete the case
        mvc.perform(delete(GET_CASE_URL, caseaseUuid))
                .andExpect(status().isOk());

        // delete non existing file
        mvc.perform(delete(GET_CASE_URL, caseaseUuid))
                .andExpect(content().string(startsWith("The directory with the following uuid doesn't exist:")))
                .andReturn();

    }

    @Test
    void test() throws Exception {
        // create the storage dir
        createStorageDir();

        // import a case
        UUID firstCaseUuid = importCase(TEST_CASE, false);

        // list the cases and expect the one imported before
        mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
                .andExpect(MockMvcResultMatchers.jsonPath("$", hasSize(1)))
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].name").value(TEST_CASE))
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].format").value(TEST_CASE_FORMAT))
                .andReturn();

        String testCaseContent = new String(ByteStreams.toByteArray(getClass().getResourceAsStream("/" + TEST_CASE)), StandardCharsets.UTF_8);

        // retrieve a case in XIIDM format
        var mvcResult = mvc.perform(post(GET_CASE_URL, firstCaseUuid).param("format", "XIIDM"))
                .andExpect(status().isOk())
                .andExpect(content().xml(testCaseContent))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_OCTET_STREAM))
                .andReturn();
        assertThat(mvcResult.getResponse().getHeader("content-disposition")).contains("attachment;");
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

        // download a case
        mvc.perform(get(GET_CASE_URL, firstCaseUuid))
                .andExpect(status().isOk())
                .andExpect(content().xml(testCaseContent))
                .andReturn();

        // export a case in CGMES format
        mvcResult = mvc.perform(post(GET_CASE_URL, firstCaseUuid).param("format", "CGMES"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_OCTET_STREAM))
                .andReturn();
        assertThat(mvcResult.getResponse().getHeader("content-disposition")).contains("attachment;");

        // delete the case
        mvc.perform(delete(GET_CASE_URL, firstCaseUuid))
                .andExpect(status().isOk());

        // import a case to delete it
        UUID secondCaseUuid = importCase(TEST_CASE, false);

        // assert that the broker message has been sent
        Message<byte[]> messageImportPrivate2 = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImportPrivate2.getPayload()));
        MessageHeaders headersPrivateCase2 = messageImportPrivate2.getHeaders();
        assertEquals(TEST_CASE, headersPrivateCase2.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(secondCaseUuid, headersPrivateCase2.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("XIIDM", headersPrivateCase2.get(CaseInfos.FORMAT_HEADER_KEY));

        //check that the case doesn't have an expiration date
        CaseMetadataEntity caseMetadataEntity = caseMetadataRepository.findById(secondCaseUuid).orElseThrow();
        assertEquals(secondCaseUuid, caseMetadataEntity.getId());
        assertNull(caseMetadataEntity.getExpirationDate());

        // delete all cases
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());

        //check that the caseMetadataRepository is empty since all cases were removed
        assertTrue(caseMetadataRepository.findAll().isEmpty());

        UUID caseUuid = importCase(TEST_CASE, false);

        // assert that the broker message has been sent
        Message<byte[]> messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        MessageHeaders headersCase = messageImport.getHeaders();
        assertEquals(TEST_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(caseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        //check that the case doesn't have an expiration date
        caseMetadataEntity = caseMetadataRepository.findById(caseUuid).orElseThrow();
        assertEquals(caseUuid, caseMetadataEntity.getId());
        assertNull(caseMetadataEntity.getExpirationDate());

        //duplicate an existing case
        MvcResult duplicateResult = mvc.perform(post("/v1/cases?duplicateFrom=" + caseUuid))
                .andExpect(status().isOk())
                .andReturn();

        String duplicateCaseUuid = duplicateResult.getResponse().getContentAsString().replace("\"", "");

        // assert that broker message has been sent after duplication
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals(UUID.fromString(duplicateCaseUuid), headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals(TEST_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        //check that the duplicated case doesn't have an expiration date
        caseMetadataEntity = caseMetadataRepository.findById(UUID.fromString(duplicateCaseUuid)).orElseThrow();
        assertEquals(duplicateCaseUuid, caseMetadataEntity.getId().toString());
        assertNull(caseMetadataEntity.getExpirationDate());

        // import a case with expiration
        Instant beforeImportDate = Instant.now().plus(1, ChronoUnit.HOURS);
        UUID thirdCaseUuid = importCase(TEST_CASE, true);
        Instant afterImportDate = Instant.now().plus(1, ChronoUnit.HOURS);

        // assert that the broker message has been sent
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals(TEST_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(thirdCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        //check that the case does have an expiration date
        caseMetadataEntity = caseMetadataRepository.findById(thirdCaseUuid).orElseThrow();
        assertEquals(thirdCaseUuid, caseMetadataEntity.getId());
        //verify that beforeImportDate < caseMetadataEntity.getExpirationDate() < afterImportDate
        assertTrue(caseMetadataEntity.getExpirationDate().isAfter(beforeImportDate));
        assertTrue(caseMetadataEntity.getExpirationDate().isBefore(afterImportDate));
        assertNotNull(caseMetadataEntity.getExpirationDate());

        //duplicate an existing case withExpiration
        MvcResult duplicateResult2 = mvc.perform(post("/v1/cases?duplicateFrom=" + caseUuid)
                .param("withExpiration", "true"))
                .andExpect(status().isOk())
                .andReturn();

        String duplicateCaseUuid2 = duplicateResult2.getResponse().getContentAsString().replace("\"", "");
        assertNotEquals(caseUuid.toString(), duplicateCaseUuid2);

        // assert that broker message has been sent after duplication
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals(UUID.fromString(duplicateCaseUuid2), headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals(TEST_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        //check that the duplicated case does have an expiration date
        caseMetadataEntity = caseMetadataRepository.findById(UUID.fromString(duplicateCaseUuid2)).orElseThrow();
        assertEquals(duplicateCaseUuid2, caseMetadataEntity.getId().toString());
        assertNotNull(caseMetadataEntity.getExpirationDate());

        //remove the expiration date of the previously duplicated case
        mvc.perform(put("/v1/cases/{caseUuid}/disableExpiration", duplicateCaseUuid2))
                .andExpect(status().isOk())
                .andReturn();

        //verify that the expiration date is removed
        caseMetadataEntity = caseMetadataRepository.findById(UUID.fromString(duplicateCaseUuid2)).orElseThrow();
        assertNull(caseMetadataEntity.getExpirationDate());

        //remove the duplicated case and check that the entry is deleted from the CaseMetadataRepository
        mvc.perform(delete("/v1/cases/{caseUuid}", duplicateCaseUuid2))
                .andExpect(status().isOk())
                .andReturn();

        assertTrue(caseMetadataRepository.findById(UUID.fromString(duplicateCaseUuid2)).isEmpty());

        //remove the expiration date of a non existing case and expect a 404
        UUID randomUuid = UUID.randomUUID();
        MvcResult deleteExpirationResult = mvc.perform(put("/v1/cases/{caseUuid}/disableExpiration", randomUuid))
                .andExpect(status().isNotFound())
                .andReturn();
        assertTrue(deleteExpirationResult.getResponse().getContentAsString().contains("case " + randomUuid + " not found"));

        // assert that duplicating a non existing case should return a 404
        mvc.perform(post("/v1/cases?duplicateFrom=" + UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andReturn();

        // list the cases and expect one case
        mvcResult = mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andReturn();

        assertTrue(mvcResult.getResponse().getContentAsString().contains("\"name\":\"testCase.xiidm\""));

        // test case metadata
        mvcResult = mvc.perform(get("/v1/cases/metadata?ids=" + caseUuid))
                .andExpect(status().isOk())
                .andReturn();
        String response = mvcResult.getResponse().getContentAsString();
        assertTrue(response.contains("\"format\":\"XIIDM\""));

        // delete all cases
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());
    }

    @Test
    void testGetMetadataOfNonExistingCase() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/metadata?ids=" + UUID.randomUUID()))
                .andExpect(status().isOk())
                .andReturn();
        String response = mvcResult.getResponse().getContentAsString();
        assertEquals("[]", response);
    }

    @Test
    void testDuplicateNonIndexedCase() throws Exception {
        // create the storage dir
        createStorageDir();

        // import IIDM test case
        String caseUuid = mvc.perform(multipart("/v1/cases")
                        .file(createMockMultipartFile(TEST_CASE))
                        .param("withExpiration", "false")
                        .param("withIndexation", "false"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
        //duplicate an existing case
        String duplicateCaseStr = mvc.perform(post("/v1/cases?duplicateFrom=" + caseUuid.substring(1, caseUuid.length() - 1)))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        UUID duplicateCaseUuid = UUID.fromString(duplicateCaseStr.substring(1, duplicateCaseStr.length() - 1));
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
        assertFalse(caseMetadataRepository.findById(duplicateCaseUuid).get().isIndexed());
    }

    UUID importCase(String testCase, Boolean withExpiration) throws Exception {
        String importedCase;
        if (withExpiration) {
            importedCase = mvc.perform(multipart("/v1/cases")
                    .file(createMockMultipartFile(testCase))
                    .param("withExpiration", withExpiration.toString())
                    .param("withIndexation", "true"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
        } else {
            importedCase = mvc.perform(multipart("/v1/cases")
                    .file(createMockMultipartFile(testCase))
                    .param("withIndexation", "true"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
        }
        return UUID.fromString(importedCase.substring(1, importedCase.length() - 1));
    }

    @Test
    void searchCaseTest() throws Exception {
        // create the storage dir
        createStorageDir();

        // delete all cases
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());

        // import IIDM test case
        String aCase = mvc.perform(multipart("/v1/cases")
                .file(createMockMultipartFile("testCase.xiidm"))
                        .param("withIndexation", "true"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        UUID aCaseUuid = UUID.fromString(aCase.substring(1, aCase.length() - 1));

        // assert that broker message has been sent and properties are the right ones
        Message<byte[]> messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        MessageHeaders headersCase = messageImport.getHeaders();
        assertEquals(TEST_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(aCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        // import CGMES french file
        aCase = mvc.perform(multipart("/v1/cases")
                .file(createMockMultipartFile("20200424T1330Z_2D_RTEFRANCE_001.zip"))
                        .param("withIndexation", "true"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        aCaseUuid = UUID.fromString(aCase.substring(1, aCase.length() - 1));

        // assert that broker message has been sent and properties are the right ones
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals("20200424T1330Z_2D_RTEFRANCE_001.zip", headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(aCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("CGMES", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        // import UCTE french file
        aCase = mvc.perform(multipart("/v1/cases")
                .file(createMockMultipartFile("20200103_0915_FO5_FR0.UCT"))
                        .param("withIndexation", "true"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        aCaseUuid = UUID.fromString(aCase.substring(1, aCase.length() - 1));

        // assert that broker message has been sent and properties are the right ones
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals("20200103_0915_FO5_FR0.UCT", headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(aCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("UCTE", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        // import UCTE german file
        aCase = mvc.perform(multipart("/v1/cases")
                .file(createMockMultipartFile("20200103_0915_SN5_D80.UCT"))
                        .param("withIndexation", "true"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        aCaseUuid = UUID.fromString(aCase.substring(1, aCase.length() - 1));

        // assert that broker message has been sent and properties are the right ones
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals("20200103_0915_SN5_D80.UCT", headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(aCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("UCTE", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        // import UCTE swiss file
        aCase = mvc.perform(multipart("/v1/cases")
                .file(createMockMultipartFile("20200103_0915_135_CH2.UCT"))
                        .param("withIndexation", "true"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        aCaseUuid = UUID.fromString(aCase.substring(1, aCase.length() - 1));

        // assert that broker message has been sent and properties are the right ones
        messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        headersCase = messageImport.getHeaders();
        assertEquals("20200103_0915_135_CH2.UCT", headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals(aCaseUuid, headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals("UCTE", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));

        // list the cases
        MvcResult mvcResult = mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andReturn();

        // assert that the 5 previously imported cases are present
        String response = mvcResult.getResponse().getContentAsString();
        assertTrue(response.contains("\"name\":\"testCase.xiidm\""));
        assertTrue(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        // search the cases
        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", "*"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertTrue(response.contains("\"name\":\"testCase.xiidm\""));
        assertTrue(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20200103_0915")))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertFalse(response.contains("\"name\":\"testCase.xiidm\""));
        assertFalse(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", "geographicalCode:(FR) OR tso:(RTEFRANCE)"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertFalse(response.contains("\"name\":\"testCase.xiidm\""));
        assertTrue(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20140116_0830") + " AND geographicalCode:(ES)"))
                .andExpect(status().isOk())
                .andReturn();
        assertEquals("[]", mvcResult.getResponse().getContentAsString());

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20140116_0830") + " AND geographicalCode:(FR)"))
                .andExpect(status().isOk())
                .andReturn();
        assertEquals("[]", mvcResult.getResponse().getContentAsString());

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20200212_1030") + " AND geographicalCode:(PT)"))
                .andExpect(status().isOk())
                .andReturn();
        assertEquals("[]", mvcResult.getResponse().getContentAsString());

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20200212_1030") + " AND geographicalCode:(FR)"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertFalse(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20200103_0915") + " AND geographicalCode:(CH)"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertFalse(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20200103_0915") + " AND geographicalCode:(FR OR CH OR D8)"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertTrue(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));
        assertFalse(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", "tso:(RTEFRANCE) AND businessProcess:(2D) AND format:(CGMES)"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertFalse(response.contains("\"name\":\"testCase.xiidm\""));
        assertTrue(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        // reindex all cases
        mvc.perform(post("/v1/supervision/cases/reindex"))
            .andExpect(status().isOk());

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", "*"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertTrue(response.contains("\"name\":\"testCase.xiidm\""));
        assertTrue(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertTrue(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));

        // delete all cases
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());

        mvcResult = mvc.perform(get("/v1/cases/search")
                .param("q", getDateSearchTerm("20200103_0915") + " AND geographicalCode:(FR OR CH OR D8)"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertFalse(response.contains("\"name\":\"20200103_0915_FO5_FR0.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_SN5_D80.UCT\""));
        assertFalse(response.contains("\"name\":\"20200103_0915_135_CH2.UCT\""));
        assertFalse(response.contains("\"name\":\"20200424T1330Z_2D_RTEFRANCE_001.zip\""));
    }

    private static String getDateSearchTerm(String entsoeFormatDate) {
        String utcFormattedDate = EntsoeFileNameParser.parseDateTime(entsoeFormatDate).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        return "date:\"" + utcFormattedDate + "\"";
    }

    abstract void addRandomFile() throws IOException;

    abstract void removeRandomFile() throws IOException;

    @Test
    void invalidFileInCaseDirectoryShouldBeIgnored() throws Exception {
        createStorageDir();

        // add a random file in the storage, not stored in a UUID named directory
        addRandomFile();

        // import a case properly
        importCase(TEST_CASE, false);

        MvcResult mvcResult = mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andReturn();
        String resultAsString = mvcResult.getResponse().getContentAsString();
        List<CaseInfos> caseInfos = mapper.readValue(resultAsString, new TypeReference<>() { });
        assertEquals(1, caseInfos.size());
        assertEquals(TEST_CASE, caseInfos.get(0).getName());

        removeRandomFile();
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }

    @Test
    void casesWithoutMetadataShouldBeIgnored() throws Exception {
        createStorageDir();

        // add a case file in a UUID named directory but no metadata in the database
        UUID caseUuid = importCase(TEST_CASE, false);
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
        caseMetadataRepository.deleteById(caseUuid);

        // import a case properly
        importCase(TEST_CASE, false);
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

        MvcResult mvcResult = mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andReturn();
        String resultAsString = mvcResult.getResponse().getContentAsString();
        List<CaseInfos> caseInfos = mapper.readValue(resultAsString, new TypeReference<>() { });
        assertEquals(1, caseInfos.size());
        assertEquals(TEST_CASE, caseInfos.get(0).getName());

        caseService.deleteCase(caseUuid);
        mvc.perform(delete("/v1/cases"))
                .andExpect(status().isOk());
    }

    @Test
    void testGetCaseBaseName() throws Exception {
        MvcResult mvcResult = mvc.perform(get("/v1/cases/caseBaseName?caseName=case.xml"))
                .andExpect(status().isOk())
                .andReturn();
        String response = mvcResult.getResponse().getContentAsString();
        assertEquals("case", response);

        mvcResult = mvc.perform(get("/v1/cases/caseBaseName?caseName=case.xml.gz"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertEquals("case", response);

        mvcResult = mvc.perform(get("/v1/cases/caseBaseName?caseName=case.v1.xml"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertEquals("case.v1", response);

        mvcResult = mvc.perform(get("/v1/cases/caseBaseName?caseName=case.v1.xml.gz"))
                .andExpect(status().isOk())
                .andReturn();
        response = mvcResult.getResponse().getContentAsString();
        assertEquals("case.v1", response);
    }

    @Test
    public void testTar() throws Exception {
        // create the storage dir
        createStorageDir();

        // import a case
        UUID tarCaseUuid = importCase(TEST_TAR_CASE, false);

        // list the cases and expect the one imported before
        mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].name").value(TEST_TAR_CASE))
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].format").value(TEST_CASE_FORMAT))
                .andReturn();

        // retrieve a case in XIIDM format
        var mvcResult = mvc.perform(post(GET_CASE_URL, tarCaseUuid).param("format", "XIIDM"))
                .andExpect(status().isOk())
                .andReturn();
        assertThat(mvcResult.getResponse().getHeader("content-disposition")).contains("attachment;");
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

        //duplicate an existing case
        MvcResult duplicateResult = mvc.perform(post("/v1/cases?duplicateFrom=" + tarCaseUuid))
                .andExpect(status().isOk())
                .andReturn();

        String duplicateCaseUuid = duplicateResult.getResponse().getContentAsString().replace("\"", "");
        // assert that broker message has been sent after duplication
        Message<byte[]> messageImport = outputDestination.receive(1000, caseImportDestination);
        assertEquals("", new String(messageImport.getPayload()));
        MessageHeaders headersCase = messageImport.getHeaders();
        assertEquals(UUID.fromString(duplicateCaseUuid), headersCase.get(CaseInfos.UUID_HEADER_KEY));
        assertEquals(TEST_TAR_CASE, headersCase.get(CaseInfos.NAME_HEADER_KEY));
        assertEquals("XIIDM", headersCase.get(CaseInfos.FORMAT_HEADER_KEY));
    }

    @Test
    void testImportCaseWithUuid() throws Exception {
        createStorageDir();

        // import a case
        mvc.perform(multipart("/v1/migration/cases")
                        .file(createMockMultipartFile(TEST_CASE))
                        .param("caseUuid", CASE_UUID_TO_IMPORT.toString()))
                .andExpect(status().isOk());

        // retrieve case format
        mvc.perform(get(GET_CASE_FORMAT_URL, CASE_UUID_TO_IMPORT))
                .andExpect(status().isOk())
                .andExpect(content().string(TEST_CASE_FORMAT));

        // import a case with existing UUID
        mvc.perform(multipart("/v1/migration/cases")
                        .file(createMockMultipartFile(TEST_CASE))
                        .param("caseUuid", CASE_UUID_TO_IMPORT.toString()))
                .andExpect(status().isConflict());

        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }
}
