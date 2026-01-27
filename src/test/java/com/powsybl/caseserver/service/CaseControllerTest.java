/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.datasource.utils.TmpMultiPartFile;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.parsers.entsoe.EntsoeFileNameParser;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
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
import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static com.powsybl.caseserver.Utils.ZIP_EXTENSION;
import static com.powsybl.caseserver.service.CaseService.DELIMITER;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
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
class CaseControllerTest implements MinioContainerConfig {
    static final String TEST_CASE = "testCase.xiidm";
    static final String TEST_CASE_2 = "test(2)Case.xiidm";
    static final String TEST_GZIP_CASE = "LF.xml.gz";
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

    @Autowired
    CaseService caseService;

    @Autowired
    CaseMetadataRepository caseMetadataRepository;

    @Autowired
    OutputDestination outputDestination;

    @Autowired
    ObjectMapper mapper;

    final String caseImportDestination = "case.import.destination";

    void addRandomFile() {
        RequestBody requestBody = RequestBody.fromString("");
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(caseService.getBucketName())
                .key(caseService.getRootDirectory() + "/randomFile.txt")
                .contentType("application/octet-stream")
                .build();
        caseService.getS3Client().putObject(putObjectRequest, requestBody);
    }

    void removeFile(String caseKey) {
        List<ObjectIdentifier> objectsToDelete = caseService.getS3Client().listObjectsV2(builder -> builder.bucket(caseService.getBucketName()).prefix(caseService.getRootDirectory() + "/" + caseKey))
                .contents()
                .stream()
                .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                .toList();
        DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                .bucket(caseService.getBucketName())
                .delete(delete -> delete.objects(objectsToDelete))
                .build();
        caseService.getS3Client().deleteObjects(deleteObjectsRequest);
    }

    @BeforeEach
    void setUp() {
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseService.deleteAllCases();
        outputDestination.clear();
    }

    private static MockMultipartFile createMockMultipartFile(String fileName) throws IOException {
        try (InputStream inputStream = CaseControllerTest.class.getResourceAsStream("/" + fileName)) {
            return new MockMultipartFile("file", fileName, MediaType.TEXT_PLAIN_VALUE, inputStream);
        }
    }

    @Test
    void testCheckNonExistingCase() throws Exception {
        // check if the case exists (except a false)
        mvc.perform(get("/v1/cases/{caseUuid}/exists", RANDOM_UUID))
                .andExpect(status().isOk())
                .andExpect(content().string("false"))
                .andReturn();
    }

    @Test
    void testImportValidCase() throws Exception {
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
        // download a non existing case
        mvc.perform(get(GET_CASE_URL, UUID.randomUUID()))
                .andExpect(status().isNoContent())
                .andReturn();
    }

    @Test
    void testDownloadCaseWithSpecialCharacters() throws Exception {
        UUID caseUuid = importCase(TEST_CASE_2, false);
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

        mvc.perform(get(GET_CASE_URL, caseUuid))
                .andExpect(status().isOk())
                .andDo(result ->
                        assertEquals(TEST_CASE_FORMAT.toLowerCase(), result.getResponse().getHeader("extension"))
            );
    }

    @Test
    void deleteNonExistingCase() throws Exception {
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
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

        // download a plain file case
        try (InputStream inputStream = getClass().getResourceAsStream("/" + TEST_CASE)) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                inputStream.transferTo(gzipOutputStream);
            }
            byte[] expectedGzippedBytes = byteArrayOutputStream.toByteArray();
            mvc.perform(get(GET_CASE_URL, firstCaseUuid))
                    .andExpect(status().isOk())
                    .andExpect(content().bytes(expectedGzippedBytes))
                    .andReturn();
        }

        UUID gzipCaseUuid = importCase(TEST_GZIP_CASE, false);
        mvc.perform(get(GET_CASE_URL, gzipCaseUuid))
                .andExpect(status().isOk())
                .andExpect(content().bytes(getClass().getResourceAsStream("/" + TEST_GZIP_CASE).readAllBytes()))
                .andReturn();
        assertNotNull(outputDestination.receive(1000, caseImportDestination));

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
        caseService.deleteAllCases();

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
        MvcResult mvcResult = mvc.perform(get("/v1/cases"))
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
        caseService.deleteAllCases();
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
        caseService.deleteAllCases();

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
        caseService.deleteAllCases();

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

    @Test
    void invalidFileInCaseDirectoryShouldBeIgnored() throws Exception {
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

        removeFile("randomFile.txt");
        caseService.deleteAllCases();
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }

    @Test
    void casesWithoutMetadataShouldBeIgnored() throws Exception {
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
        caseService.deleteAllCases();
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
    void testTar() throws Exception {
        // import a case
        UUID tarCaseUuid = importCase(TEST_TAR_CASE, false);

        // list the cases and expect the one imported before
        mvc.perform(get("/v1/cases"))
                .andExpect(status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("$").isArray())
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].name").value(TEST_TAR_CASE))
                .andExpect(MockMvcResultMatchers.jsonPath("$[0].format").value(TEST_CASE_FORMAT))
                .andReturn();

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

    @Test
    void testDownloadInvalidCase() {
        UUID caseUuid = UUID.randomUUID();
        caseService.createCaseMetadataEntity(caseUuid, false, false, "testCase.xiidm", null, "XIIDM");
        assertEquals(Optional.empty(), caseService.getCaseStream(caseUuid));
    }

    @Test
    void testDuplicate() throws Exception {
        UUID firstCaseUuid = importCase(TEST_CASE, false);
        removeFile(firstCaseUuid.toString());
        assertThrows(ResponseStatusException.class, () -> caseService.duplicateCase(firstCaseUuid, false));
        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }

    void addZipCaseFile(UUID caseUuid, String folderName, String fileName) throws IOException {
        try (InputStream inputStream = CaseControllerTest.class.getResourceAsStream("/" + fileName + ZIP_EXTENSION)) {
            if (inputStream != null) {
                RequestBody requestBody = RequestBody.fromBytes(inputStream.readAllBytes());
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(caseService.getBucketName())
                    .key(folderName + DELIMITER + caseUuid + DELIMITER + fileName + ZIP_EXTENSION)
                    .contentType("application/zip")
                    .build();
                caseService.getS3Client().putObject(putObjectRequest, requestBody);
            }
        }
    }

    @Test
    void testCreateCase() throws Exception {

        UUID caseUuid = UUID.randomUUID();
        String folderName = "network_exports";
        String fileName = "zippedTestCase";

        // create zip case in one folder in bucket
        addZipCaseFile(caseUuid, folderName, fileName);

        mvc.perform(post("/v1/cases")
                .param("caseKey", folderName + DELIMITER + caseUuid + DELIMITER + fileName + ZIP_EXTENSION)
                .param("contentType", "application/zip"))
            .andExpect(status().isOk());

        assertNotNull(outputDestination.receive(1000, caseImportDestination));
    }

    @Test
    void testCreateCaseKo() throws Exception {

        UUID caseUuid = UUID.randomUUID();
        String folderName = "network_exports";
        String fileName = "testCase4";

        mvc.perform(post("/v1/cases")
                .param("caseKey", folderName + DELIMITER + caseUuid + DELIMITER + fileName)
                .param("contentType", "application/zip"))
            .andExpect(status().isInternalServerError());
    }

    @Test
    void testS3MultiPartFile() throws IOException {
        UUID caseUuid = UUID.randomUUID();
        String folderName = "network_exports";
        String fileName = "zippedTestCase";

        // create zip case in one folder in bucket
        addZipCaseFile(caseUuid, folderName, fileName);

        String caseKey = folderName + DELIMITER + caseUuid + DELIMITER + fileName + ZIP_EXTENSION;
        InputStream inputStream = caseService.getCaseStream(caseKey).get();
        try (TmpMultiPartFile file = new TmpMultiPartFile(inputStream, caseKey, "application/zip")) {
            try (InputStream in = CaseControllerTest.class.getResourceAsStream("/" + fileName + ZIP_EXTENSION)) {
                assertNotNull(in);
                byte[] bytes = in.readAllBytes();
                Assertions.assertEquals(bytes.length, file.getSize());
                Assertions.assertEquals("application/zip", file.getContentType());
                assertFalse(file.isEmpty());
                File tmpFile = new File("/tmp/testFile.zip");
                file.transferTo(tmpFile);
                assertTrue(tmpFile.exists());
                tmpFile.delete();
            }
        }
    }
}
