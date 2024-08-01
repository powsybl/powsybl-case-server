/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.caseserver.services.SupervisionService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {"case-store-directory=/cases"})
@ContextConfigurationWithTestChannel
public class SupervisionTest {
    @Autowired
    SupervisionService supervisionService;
    @Autowired
    CaseInfosRepository caseInfosRepository;
    @Autowired
    CaseMetadataRepository caseMetadataRepository;
    @Autowired
    CaseService caseService;

    @Autowired
    private MockMvc mvc;

    @Test
    public void testGetElementInfosCount() {
        supervisionService.getIndexedCaseElementsCount();
        verify(caseInfosRepository, times(1)).count();
    }

    @Test
    public void testDeleteElementInfos() {
        supervisionService.deleteIndexedDirectoryElements();

        verify(caseInfosRepository, times(1)).count();
        verify(caseInfosRepository, times(1)).deleteAll();
    }


}
