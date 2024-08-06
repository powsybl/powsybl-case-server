/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.services;

import com.powsybl.caseserver.CaseService;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@Service
public class SupervisionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupervisionService.class);

    private final CaseInfosService caseInfosService;
    private final CaseService caseService;
    private final CaseInfosRepository caseInfosRepository;

    public SupervisionService(CaseInfosService caseInfosService, CaseService caseService, CaseInfosRepository caseInfosRepository) {
        this.caseInfosService = caseInfosService;
        this.caseService = caseService;
        this.caseInfosRepository = caseInfosRepository;
    }

    public void reindexAllCases() {
        List<CaseInfos> allCases = caseService.getAllCases();
        Set<UUID> casesToIndex = caseService.getCaseToReindex();
        List<CaseInfos> data = allCases.stream().filter(c -> casesToIndex.contains(c.getUuid())).toList();
        caseInfosService.recreateAllCaseInfos(data);
    }

    public long deleteIndexedDirectoryElements() {
        AtomicReference<Long> startTime = new AtomicReference<>();
        startTime.set(System.nanoTime());

        long nbIndexesToDelete = getIndexedCaseElementsCount();
        caseInfosRepository.deleteAll();
        LOGGER.trace("Indexed directory elements deletion : {} seconds", TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime.get()));
        return nbIndexesToDelete;
    }

    public long getIndexedCaseElementsCount() {
        return caseInfosRepository.count();
    }
}
