package com.powsybl.caseserver.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import java.util.UUID;

import static com.powsybl.caseserver.Utils.ZIP_EXTENSION;
import static com.powsybl.caseserver.service.CaseService.DELIMITER;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
class MockedCaseControllerTest {

    @MockitoBean
    CaseService spyCaseService;

    @Spy
    private S3Client s3Client;

    @Autowired
    protected MockMvc mvc;

    private static final String CREATE_CASE_URL = "/v1/cases/create";

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
    }

    @Test
    void testCreateCase() throws Exception {

        UUID caseUuid = UUID.randomUUID();
        String folderName = "test-folder";
        String fileName = "test-file";
        String caseFileKey = folderName + DELIMITER + caseUuid + DELIMITER + fileName + ZIP_EXTENSION;

        mockS3ClientGetObject(caseFileKey);

        InputStream inputStream = new ByteArrayInputStream(new byte[]{});
        when(spyCaseService.getInputStreamFromS3(caseUuid, folderName, fileName + ZIP_EXTENSION))
            .thenReturn(Optional.of(inputStream));

        mvc.perform(post(CREATE_CASE_URL)
                .param("caseUuid", caseUuid.toString())
                .param("folderName", folderName)
                .param("fileName", fileName))
            .andExpect(status().isOk());

        verify(spyCaseService).getInputStreamFromS3(caseUuid, folderName, fileName + ZIP_EXTENSION);

    }

    private void mockS3ClientGetObject(String caseFileKey) {
        GetObjectRequest mockRequest = GetObjectRequest.builder()
            .bucket("ws-bucket")
            .key(caseFileKey)
            .build();

        when(s3Client.getObject(mockRequest))
            .thenReturn(mock(ResponseInputStream.class));
    }
}
