package com.powsybl.caseserver.service;

import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.FileNotFoundException;
import java.util.UUID;

import static org.junit.Assert.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisableElasticsearch
public class S3CaseServiceTest implements MinioContainerConfig {

    @Autowired
    private S3CaseService s3CaseService;

    private static final String TEST_OTHER_CASE_FILE_NAME = "testCase.xiidm";

    @Test
    public void duplicateInvalidCase() {
        UUID caseUuid = UUID.randomUUID();
        s3CaseService.createCaseMetadataEntity(caseUuid, false, false, TEST_OTHER_CASE_FILE_NAME, null, "XIIDM");
        assertThrows(FileNotFoundException.class, () -> s3CaseService.duplicateCase(caseUuid, false));
    }
}
