/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.services.SupervisionService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Jamal KHEYYAD <jamal.kheyyad at rte-international.com>
 */
@RestController
@RequestMapping(value = "/" + CaseConstants.API_VERSION + "/supervision")
@Tag(name = "case-server - Supervision")
public class SupervisionController {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupervisionController.class);

    private final SupervisionService supervisionService;
    private final ClientConfiguration elasticsearchClientConfiguration;
    private final CaseInfosService caseInfosService;

    public SupervisionController(SupervisionService supervisionService, ClientConfiguration elasticsearchClientConfiguration, CaseInfosService caseInfosService) {
        this.supervisionService = supervisionService;
        this.elasticsearchClientConfiguration = elasticsearchClientConfiguration;
        this.caseInfosService = caseInfosService;
    }

    @PostMapping(value = "/cases/reindex-all")
    @Operation(summary = "reindex all cases")
    public ResponseEntity<Void> reindexAllCases() {
        LOGGER.debug("reindex all cases request received");
        supervisionService.reindexAllCases();
        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/elasticsearch-host")
    @Operation(summary = "get the elasticsearch address")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "the elasticsearch address")})
    public ResponseEntity<String> getElasticsearchHost() {
        String host = elasticsearchClientConfiguration.getEndpoints().get(0).getHostName()
                + ":"
                + elasticsearchClientConfiguration.getEndpoints().get(0).getPort();
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(host);
    }

    @GetMapping(value = "/elements/index-name")
    @Operation(summary = "get the indexed directory elements index name")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Indexed directory elements index name")})
    public ResponseEntity<String> getIndexedDirectoryElementsIndexName() {
        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(caseInfosService.getDirectoryElementsIndexName());
    }

//    @GetMapping(value = "/elements/indexation-count")
//    @Operation(summary = "get indexed directory elements count")
//    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Indexed directory elements count")})
//    public ResponseEntity<String> getIndexedDirectoryElementsCount() {
//        return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(Long.toString(caseInfosService.getIndexedDirectoryElementsCount()));
//    }

}
