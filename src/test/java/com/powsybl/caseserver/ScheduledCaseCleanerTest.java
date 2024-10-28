/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@SpringBootTest
@DisableElasticsearch
class ScheduledCaseCleanerTest {

    @Autowired
    private CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private ScheduledCaseCleaner scheduledCaseCleaner;

    @MockBean
    private CaseInfosRepository caseInfosRepository;

    @MockBean
    private CaseService caseService;

    @AfterEach
    void cleanDB() {
        caseMetadataRepository.deleteAll();
    }

    @Test
    void test() {
        Instant now = Instant.now();
        Instant yesterday = now.minus(1, ChronoUnit.DAYS);
        CaseMetadataEntity shouldNotExpireEntity = new CaseMetadataEntity(UUID.randomUUID(), now.plus(1, ChronoUnit.HOURS), false);
        CaseMetadataEntity shouldExpireEntity = new CaseMetadataEntity(UUID.randomUUID(), yesterday.plus(1, ChronoUnit.HOURS), false);
        CaseMetadataEntity noExpireDateEntity = new CaseMetadataEntity(UUID.randomUUID(), null, false);
        caseMetadataRepository.save(shouldExpireEntity);
        caseMetadataRepository.save(shouldNotExpireEntity);
        caseMetadataRepository.save(noExpireDateEntity);
        assertEquals(3, caseMetadataRepository.findAll().size());
        scheduledCaseCleaner.deleteExpiredCases();
        assertEquals(2, caseMetadataRepository.findAll().size());
        assertTrue(caseMetadataRepository.findById(shouldNotExpireEntity.getId()).isPresent());
        assertTrue(caseMetadataRepository.findById(noExpireDateEntity.getId()).isPresent());
        assertTrue(caseMetadataRepository.findById(shouldExpireEntity.getId()).isEmpty());
        verify(caseService, times(1)).deleteCase(shouldExpireEntity.getId());
    }
}
