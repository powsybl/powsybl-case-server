/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.service.SupervisionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@RestController
@RequestMapping(value = "/" + CaseConstants.API_VERSION + "/supervision")
@Tag(name = "case-server - Supervision")
public class SupervisionController {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupervisionController.class);

    private final SupervisionService supervisionService;
    private final CaseService caseService;
    private final RestClient restClient;
    private final CaseInfosService caseInfosService;

    public SupervisionController(SupervisionService supervisionService, CaseService caseService, RestClient restClient, CaseInfosService caseInfosService) {
        this.supervisionService = supervisionService;
        this.caseService = caseService;
        this.restClient = restClient;
        this.caseInfosService = caseInfosService;
    }

    @GetMapping(value = "/elasticsearch-host")
    @Operation(summary = "get the elasticsearch address")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "the elasticsearch address")})
    public ResponseEntity<String> getElasticsearchHost() {
        HttpHost httpHost = restClient.getNodes().get(0).getHost();
        String host = httpHost.getHostName()
                + ":"
                + httpHost.getPort();
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(host);
    }

    @GetMapping(value = "/cases/index-name")
    @Operation(summary = "get the indexed cases index name")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Indexed directory cases index name")})
    public ResponseEntity<String> getIndexedCasesFIndexName() {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(caseInfosService.getDirectoryCasesIndexName());
    }

    @PostMapping(value = "/cases/reindex")
    @Operation(summary = "reindex all cases")
    public ResponseEntity<Void> reindexAllCases() {
        LOGGER.debug("reindex all cases request received");
        caseInfosService.recreateAllCaseInfos(caseService.getCasesToReindex());
        return ResponseEntity.ok().build();
    }

    @DeleteMapping(value = "/cases/indexation")
    @Operation(summary = "delete indexed cases")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "all indexed cases have been deleted")})
    public ResponseEntity<String> deleteIndexedCases() {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(Long.toString(supervisionService.deleteIndexedCases()));
    }

    @GetMapping(value = "/cases/indexation-count")
    @Operation(summary = "get indexed cases count")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Indexed cases count")})
    public ResponseEntity<String> getIndexedCasesCount() {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(Long.toString(supervisionService.getIndexedCasesCount()));
    }

}
