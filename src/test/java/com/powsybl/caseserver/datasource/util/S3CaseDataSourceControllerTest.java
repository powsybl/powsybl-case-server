/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource.util;

import com.powsybl.caseserver.ContextConfigurationWithTestChannel;
import com.powsybl.caseserver.ObjectStorageService;
import com.powsybl.commons.datasource.DataSource;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=S3"})
@ContextConfigurationWithTestChannel
public class S3CaseDataSourceControllerTest extends AbstractCaseDataSourceControllerTest {

    // TODO MOVE THIS TO SEPARATE CLASS
    private static final String MINIO_DOCKER_IMAGE_NAME = "minio/minio";
    // Just a fixed version, latest at the time of writing this
    private static final String MINIO_DOCKER_IMAGE_VERSION = "RELEASE.2023-09-27T15-22-50Z";
    private static GenericContainer minioContainer;
    private static final int MINIO_PORT = 9000;

    // can't use a bean because we need it before spring aws autoconfiguration TODO
    static {
        minioContainer = new GenericContainer(
                String.format("%s:%s", MINIO_DOCKER_IMAGE_NAME, MINIO_DOCKER_IMAGE_VERSION))
                // .withClasspathResourceMapping("/", "/data/", BindMode.READ_WRITE)
                .withCommand("server /data").withExposedPorts(MINIO_PORT).waitingFor(new HttpWaitStrategy()
                        .forPath("/minio/health/ready").forPort(MINIO_PORT).withStartupTimeout(Duration.ofSeconds(10)));
        minioContainer.start();
        try {
            minioContainer.execInContainer("mkdir", "/data/bucket-gridsuite");
        } catch (Exception e) {
            System.out.println("Error");
        }

        // can't use System.setProperty because spring cloud aws gets the url early
        // System.setProperty("spring.cloud.aws.s3.endpoint", minioContainerUrl);
    }

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        Integer mappedPort = minioContainer.getFirstMappedPort();
        Testcontainers.exposeHostPorts(mappedPort);
        String minioContainerUrl = String.format("http://172.17.0.1:%s", mappedPort);

        registry.add("spring.cloud.aws.endpoint", () -> minioContainerUrl);
    }
    // END TODO

    @MockBean
    private S3Client s3Client;

    @Autowired
    ObjectStorageService objectStorageService;

    @MockBean
    ListObjectsV2Iterable listObjectsV2Iterable;

    private static final String CASES_PREFIX = "gsi-cases/";

    @Before
    public void setUp() throws URISyntaxException {

        // TODO : mock S3CaseDataSourceService methods

        final var key = CASES_PREFIX + CASE_UUID + "/" + cgmesName;

        S3Object objectSummary1 = mock(S3Object.class);

        // Create a mock response for listObjectsV2Paginator

        List<S3Object> s3Objects = Arrays.asList(
                S3Object.builder().key(key).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Objects)
                .build();

        // Mock the behavior of listObjectsV2Paginator
        when(s3Client.listObjectsV2Paginator((ListObjectsV2Request) any())).thenReturn(listObjectsV2Iterable);
        when(listObjectsV2Iterable.iterator()).thenReturn(Collections.singletonList(response).iterator());

        // Mocking s3Client.getObject() method
        when(s3Client.getObject(any(GetObjectRequest.class), any(Path.class))).then(invocation -> {
            Path file = invocation.getArgument(1);
            try (InputStream cgmesURL = getClass().getResourceAsStream("/" + cgmesName)) {
                Files.copy(cgmesURL, file, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                // Handle the exception appropriately
                e.printStackTrace();
            }
            return null;
        });

        // Mocking s3Object.key() method
        when(objectSummary1.key()).thenReturn("gsi-cases/" + CASE_UUID + "/" + cgmesName);

        dataSource = DataSource.fromPath(Paths.get(getClass().getResource("/" + cgmesName).toURI()));

    }
}
