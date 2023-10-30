/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {"case-store-directory=/cases"})
@TestPropertySource(properties = {"storage.type=S3"})
@ContextConfigurationWithTestChannel
public class S3CaseControllerTest extends AbstractCaseControllerTest {

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
}
