/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@Service
public class SupervisionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupervisionService.class);

    private final CaseInfosRepository caseInfosRepository;

    public SupervisionService(CaseInfosRepository caseInfosRepository) {
        this.caseInfosRepository = caseInfosRepository;
    }

    public long deleteIndexedCases() {
        AtomicReference<Long> startTime = new AtomicReference<>();
        startTime.set(System.nanoTime());

        long nbIndexesToDelete = getIndexedCasesCount();
        caseInfosRepository.deleteAll();
        LOGGER.trace("Indexed cases deletion : {} seconds", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime.get()));
        return nbIndexesToDelete;
    }

    public long getIndexedCasesCount() {
        return caseInfosRepository.count();
    }
}
