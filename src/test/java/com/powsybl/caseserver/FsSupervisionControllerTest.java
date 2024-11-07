/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.service.FsCaseService;
import com.powsybl.computation.ComputationManager;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {"case-store-directory=/cases"})
@TestPropertySource(properties = {"storage.type=FS"})
class FsSupervisionControllerTest extends AbstractSupervisionControllerTest {

    @Autowired
    private FsCaseService fsCaseService;

    @BeforeEach
    public void setUp() {
        caseService = fsCaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ((FsCaseService) caseService).setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseMetadataRepository.deleteAll();
    }
}
