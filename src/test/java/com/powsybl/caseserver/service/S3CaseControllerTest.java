/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@TestPropertySource(properties = {"storage.type=S3"})
class S3CaseControllerTest extends AbstractCaseControllerTest implements MinioContainerConfig {

    @Autowired
    private S3CaseService s3CaseService;

    @BeforeEach
    void setUp() {
        caseService = s3CaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseService.deleteAllCases();
        outputDestination.clear();
    }
}
