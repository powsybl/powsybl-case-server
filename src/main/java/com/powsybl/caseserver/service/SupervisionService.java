/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosRepository;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@Service
public class SupervisionService {

    private final CaseInfosRepository caseInfosRepository;
    private final ElasticsearchOperations elasticsearchOperations;

    public SupervisionService(CaseInfosRepository caseInfosRepository, ElasticsearchOperations elasticsearchOperations) {
        this.caseInfosRepository = caseInfosRepository;
        this.elasticsearchOperations = elasticsearchOperations;
    }

    public long getIndexedCasesCount() {
        return caseInfosRepository.count();
    }

    public void recreateIndex() {
        IndexOperations indexOperations = elasticsearchOperations.indexOps(CaseInfos.class);
        boolean isDeleted = indexOperations.delete();
        if (!isDeleted) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Failed to delete cases ElasticSearch index");
        }

        boolean isCreated = indexOperations.createWithMapping();
        if (!isCreated) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Failed to create cases ElasticSearch index");
        }
    }
}
