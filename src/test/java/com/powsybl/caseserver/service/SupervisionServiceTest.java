/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

/**
 * @author Antoine Bouhours <antoine.bouhours at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisableElasticsearch
class SupervisionServiceTest {

    @MockBean
    ElasticsearchOperations elasticsearchOperations;

    @MockBean
    IndexOperations indexOperations;

    @Autowired
    SupervisionService supervisionService;

    @Test
    void recreateIndexThrowsExceptionWhenDeleteFails() {
        when(elasticsearchOperations.indexOps(CaseInfos.class)).thenReturn(indexOperations);
        when(indexOperations.delete()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> supervisionService.recreateIndex());

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getStatusCode());
        assertEquals("Failed to delete cases ElasticSearch index", exception.getReason());
        verify(elasticsearchOperations, times(1)).indexOps(CaseInfos.class);
        verify(indexOperations, times(1)).delete();
        verify(indexOperations, never()).createWithMapping();
    }

    @Test
    void recreateIndexThrowsExceptionWhenCreateFails() {
        when(elasticsearchOperations.indexOps(CaseInfos.class)).thenReturn(indexOperations);
        when(indexOperations.delete()).thenReturn(true);
        when(indexOperations.createWithMapping()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () -> supervisionService.recreateIndex());

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, exception.getStatusCode());
        assertEquals("Failed to create cases ElasticSearch index", exception.getReason());
        verify(elasticsearchOperations, times(1)).indexOps(CaseInfos.class);
        verify(indexOperations, times(1)).delete();
        verify(indexOperations, times(1)).createWithMapping();
    }

    @Test
    void recreateIndexSuccess() {
        when(elasticsearchOperations.indexOps(CaseInfos.class)).thenReturn(indexOperations);
        when(indexOperations.delete()).thenReturn(true);
        when(indexOperations.createWithMapping()).thenReturn(true);

        supervisionService.recreateIndex();

        verify(elasticsearchOperations, times(1)).indexOps(CaseInfos.class);
        verify(indexOperations, times(1)).delete();
        verify(indexOperations, times(1)).createWithMapping();
    }

    @AfterEach
    void verifyNoMoreInteractionsMocks() {
        verifyNoMoreInteractions(elasticsearchOperations);
        verifyNoMoreInteractions(indexOperations);
    }
}
