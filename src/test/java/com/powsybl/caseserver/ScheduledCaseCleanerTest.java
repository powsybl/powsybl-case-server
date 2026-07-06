/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.caseserver.service.CaseService;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@SpringBootTest
@DisableElasticsearch
@Import(DisableElasticsearch.MockConfig.class)
class ScheduledCaseCleanerTest {

    @Autowired
    private CaseMetadataRepository caseMetadataRepository;

    @Autowired
    private ScheduledCaseCleaner scheduledCaseCleaner;

    @Autowired
    private LockProvider lockProvider;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoBean
    private CaseService caseService;

    private static final String LOCK_NAME = "ScheduledCaseCleaner_deleteExpiredCases";

    @BeforeEach
    void cleanDBBeforeEach() {
        caseMetadataRepository.deleteAll();
        releaseLock();
    }

    @AfterEach
    void cleanDBAfterEach() {
        caseMetadataRepository.deleteAll();
        releaseLock();
    }

    // We need to update the lock and set the expire date as now (basically the past)
    // Just deleting the row from the database won't work. The JDBC lock provider caches
    // known lock names in memory, so deleting the row behind its back would make later lock() calls skip the insert.
    // this results in the lock being held by the lock provider and not being available for acquisition after the supposed release.
    private void releaseLock() {
        jdbcTemplate.update("UPDATE shedlock SET lock_until = ? WHERE name = ?", Timestamp.from(Instant.EPOCH), LOCK_NAME);
    }

    @Test
    void test() {
        Instant now = Instant.now();
        Instant yesterday = now.minus(1, ChronoUnit.DAYS);
        CaseMetadataEntity shouldNotExpireEntity = new CaseMetadataEntity(UUID.randomUUID(), now.plus(1, ChronoUnit.HOURS), false, "originalName", "compressionFormat", "format");
        CaseMetadataEntity shouldExpireEntity = new CaseMetadataEntity(UUID.randomUUID(), yesterday.plus(1, ChronoUnit.HOURS), false, "originalName", "compressionFormat", "format");
        CaseMetadataEntity noExpireDateEntity = new CaseMetadataEntity(UUID.randomUUID(), null, false, "originalName", "compressionFormat", "format");
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

    @Test
    void shouldSkipExecutionWhenLockAlreadyHeld() {
        // An expired case that WOULD be deleted if the job body ran.
        Instant now = Instant.now();
        CaseMetadataEntity shouldExpireEntity = new CaseMetadataEntity(UUID.randomUUID(), now.minus(1, ChronoUnit.HOURS), false, "originalName", "compressionFormat", "format");
        caseMetadataRepository.save(shouldExpireEntity);

        // Simulate another pod already holding the lock (lockAtLeastFor=0 so we can release it right after).
        Optional<SimpleLock> heldLock = lockProvider.lock(new LockConfiguration(now, LOCK_NAME, Duration.ofMinutes(10), Duration.ZERO));
        assertTrue(heldLock.isPresent(), "The lock should be acquirable");
        try {
            scheduledCaseCleaner.deleteExpiredCases();

            // Lock was held by "another pod", so the body must not have run.
            assertEquals(1, caseMetadataRepository.findAll().size());
            assertTrue(caseMetadataRepository.findById(shouldExpireEntity.getId()).isPresent());
            verify(caseService, never()).deleteCase(any());
        } finally {
            heldLock.get().unlock();
        }

        // Once the lock is released, the job runs normally and deletes the expired case.
        scheduledCaseCleaner.deleteExpiredCases();
        assertTrue(caseMetadataRepository.findById(shouldExpireEntity.getId()).isEmpty());
        verify(caseService, times(1)).deleteCase(shouldExpireEntity.getId());
    }
}
