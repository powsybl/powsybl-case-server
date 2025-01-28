package com.powsybl.caseserver.migration;

import com.powsybl.caseserver.service.CaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

@Service
public class S3MigrationCaseService {

    private static final String CASE_SERVER_API_VERSION = "v1";

    private static final String DELIMITER = "/";
    private static final String INCORRECT_CASE_FILE = "Incorrect case file";
    private final RestTemplate restTemplate;
    private String migrationCaseServerBaseUri;

    @Autowired
    private CaseService caseService;

    public S3MigrationCaseService(@Value("${powsybl.services.case-server.base-uri:http://case-server2/}") String migrationCaseServerBaseUri, RestTemplate restTemplate) {
        this.migrationCaseServerBaseUri = migrationCaseServerBaseUri;
        this.restTemplate = restTemplate;
    }

    public void setBaseUri(String migrationCaseServerBaseUri) {
        this.migrationCaseServerBaseUri = migrationCaseServerBaseUri;
    }

    UUID migrateCase(UUID caseUuid) throws Exception {
        Resource resource = new ByteArrayResource(caseService.getCaseBytes(caseUuid).get());
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        HttpHeaders headers = new HttpHeaders();
        UUID newCaseUuid;
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        if (resource != null) {
            body.add("file", resource);
        }
        HttpEntity<MultiValueMap<String, Object>> request = new HttpEntity<>(
                body, headers);
        try {
            newCaseUuid = restTemplate.postForObject(migrationCaseServerBaseUri + "/" + CASE_SERVER_API_VERSION + "/cases", request,
                    UUID.class);
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().equals(HttpStatus.UNPROCESSABLE_ENTITY)) {
                throw new Exception(e.getMessage(), e);
            }
            throw new Exception(e.getMessage(), e);
        }
        return newCaseUuid;
    }

}
