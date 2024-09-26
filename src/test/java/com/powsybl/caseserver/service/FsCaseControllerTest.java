/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.powsybl.caseserver.ContextConfigurationWithTestChannel;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.powsybl.computation.ComputationManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@TestPropertySource(properties = {"storage.type=file"})
@ContextConfigurationWithTestChannel
public class FsCaseControllerTest extends AbstractCaseControllerTest {

    @Autowired
    private FsCaseService fsCaseService;

    @Before
    public void setUp() {
        caseService = fsCaseService;
        fileSystem = Jimfs.newFileSystem(Configuration.unix());
        ((FsCaseService) caseService).setFileSystem(fileSystem);
        caseService.setComputationManager(Mockito.mock(ComputationManager.class));
        caseMetadataRepository.deleteAll();
        outputDestination.clear();
    }

    @Test
    public void testStorageNotCreated() throws Exception {
        // expect a fail since the storage dir. is not created
        mvc.perform(delete("/v1/cases")).andExpect(status().isUnprocessableEntity());
    }

}
