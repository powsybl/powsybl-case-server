/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.elasticsearch;

import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import com.google.common.collect.Lists;
import com.powsybl.caseserver.dto.CaseInfos;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A class to implement metadatas transfer in the DB elasticsearch
 *
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
@ComponentScan(basePackageClasses = {CaseInfosRepository.class})
@Service
public class CaseInfosService {

    @Autowired
    private CaseInfosRepository caseInfosRepository;

    @Autowired
    private ElasticsearchOperations operations;

    @Value(ESConfig.CASE_INFOS_INDEX_NAME)
    @Getter
    private String directoryElementsIndexName;

    public CaseInfos addCaseInfos(@NonNull final CaseInfos ci) {
        caseInfosRepository.save(ci);
        return ci;
    }

    public Optional<CaseInfos> getCaseInfosByUuid(@NonNull final String uuid) {
        Page<CaseInfos> res = caseInfosRepository.findByUuid(uuid, PageRequest.of(0, 1));
        return res.get().findFirst();
    }

    public List<CaseInfos> getAllCaseInfos() {
        return Lists.newArrayList(caseInfosRepository.findAll());
    }

    public List<CaseInfos> searchCaseInfos(@NonNull final String query) {
        NativeQuery searchQuery = new NativeQueryBuilder().withQuery(QueryStringQuery.of(qs -> qs.query(query))._toQuery()).build();
        return Lists.newArrayList(operations.search(searchQuery, CaseInfos.class)
                                            .map(SearchHit::getContent));
    }

    public void deleteCaseInfos(@NonNull final CaseInfos ci) {
        caseInfosRepository.delete(ci);
    }

    public void deleteCaseInfosByUuid(@NonNull String uuid) {
        caseInfosRepository.deleteById(uuid);
    }

    public void deleteAllCaseInfos() {
        caseInfosRepository.deleteAll(getAllCaseInfos());
    }

    public void recreateAllCaseInfos(List<CaseInfos> caseInfos) {
        caseInfosRepository.deleteAll();
        caseInfosRepository.saveAll(caseInfos);
    }

    public static String getDateSearchTerm(@NonNull final ZonedDateTime... dates) {
        return Arrays.stream(dates).map(date -> "\"" + date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME) + "\"").collect(Collectors.joining(" OR ", "date:", ""));
    }
}
