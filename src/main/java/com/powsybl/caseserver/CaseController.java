/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.service.MetadataService;
import com.powsybl.commons.datasource.DataSourceUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.*;

import static com.powsybl.caseserver.CaseException.createDirectoryNotFound;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@RestController
@RequestMapping(value = "/" + CaseConstants.API_VERSION)
@Tag(name = "Case server")
@ComponentScan(basePackageClasses = {CaseService.class})
public class CaseController {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaseController.class);

    @Autowired
    @Qualifier("storageService")
    private CaseService caseService;

    @Autowired
    private CaseInfosService caseInfosService;

    @Autowired
    private MetadataService metadataService;

    @GetMapping(value = "/cases")
    @Operation(summary = "Get all cases")
    //For maintenance purpose
    public ResponseEntity<List<CaseInfos>> getCases() {
        LOGGER.debug("getCases request received");
        List<CaseInfos> cases = caseService.getCases();
        if (cases == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok().body(cases);
    }

    @GetMapping(value = "/cases/{caseUuid}/infos")
    @Operation(summary = "Get a case infos")
    public ResponseEntity<CaseInfos> getCaseInfos(@PathVariable("caseUuid") UUID caseUuid) {
        LOGGER.debug("getCaseInfos request received");
        if (!caseService.caseExists(caseUuid)) {
            return ResponseEntity.noContent().build();
        }
        CaseInfos caseInfos = caseService.getCaseInfos(caseUuid);
        return ResponseEntity.ok().body(caseInfos);
    }

    @GetMapping(value = "/cases/{caseUuid}/format")
    @Operation(summary = "Get case Format")
    public ResponseEntity<String> getCaseFormat(@PathVariable("caseUuid") UUID caseUuid) {
        LOGGER.debug("getCaseFormat request received");
        if (!caseService.caseExists(caseUuid)) {
            throw createDirectoryNotFound(caseUuid);
        }
        String caseFormat = caseService.getFormat(caseUuid);
        return ResponseEntity.ok().body(caseFormat);
    }

    @GetMapping(value = "/cases/{caseUuid}/name")
    @Operation(summary = "Get case name")
    public ResponseEntity<String> getCaseName(@PathVariable("caseUuid") UUID caseUuid) {
        LOGGER.debug("getCaseName request received");
        if (!caseService.caseExists(caseUuid)) {
            throw createDirectoryNotFound(caseUuid);
        }
        String caseName = caseService.getCaseName(caseUuid);
        return ResponseEntity.ok().body(caseName);
    }

    @GetMapping(value = "/cases/{caseUuid}")
    @Operation(summary = "Download a case")
    public ResponseEntity<byte[]> downloadCase(@PathVariable("caseUuid") UUID caseUuid) {
        LOGGER.debug("getCase request received with parameter caseUuid = {}", caseUuid);
        Boolean isGzip = caseService.isTheFileOriginallyGzipped(caseUuid);
        byte[] bytes = caseService.getCaseBytes(caseUuid).orElse(null);
        String name = caseService.getDownloadCaseName(caseUuid);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", name);
        MediaType mediaType = Boolean.TRUE.equals(isGzip) ? MediaType.APPLICATION_OCTET_STREAM : MediaType.MULTIPART_FORM_DATA;
        if (bytes == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok()
                .headers(headers)
                .contentType(mediaType)
                .body(bytes);

    }

    @PostMapping(value = "/cases/{caseUuid}", consumes = "application/json")
    @Operation(summary = "Export a case",
            requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "Parameters for chosen format",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = Properties.class))
            )
    )
    public ResponseEntity<byte[]> exportCase(
            @PathVariable UUID caseUuid,
            @RequestParam String format,
            @RequestParam(value = "fileName", required = false) String fileName,
            @RequestBody(required = false) Map<String, Object> formatParameters) throws IOException {
        LOGGER.debug("exportCase request received with parameter caseUuid = {}", caseUuid);
        return caseService.exportCase(caseUuid, format, fileName, formatParameters).map(networkInfos -> {
            var headers = new HttpHeaders();
            headers.setContentDisposition(
                    ContentDisposition.builder("attachment")
                    .filename(networkInfos.networkName())
                    .build()
            );
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(networkInfos.networkData());
        }).orElse(ResponseEntity.noContent().build());
    }

    @GetMapping(value = "/cases/{caseUuid}/exists")
    @Operation(summary = "check if the case exists")
    public ResponseEntity<Boolean> exists(@PathVariable("caseUuid") UUID caseUuid) {
        boolean exists = caseService.caseExists(caseUuid);
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(exists);

    }

    @PostMapping(value = "/cases", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "import a case")
    @SuppressWarnings("javasecurity:S5145")
    public ResponseEntity<UUID> importCase(@RequestParam("file") MultipartFile file,
                                           @RequestParam(value = "withExpiration", required = false, defaultValue = "false") boolean withExpiration,
                                           @RequestParam(value = "withIndexation", required = false, defaultValue = "false") boolean withIndexation) {
        LOGGER.debug("importCase request received with file = {}", file.getName());
        UUID caseUuid = caseService.importCase(file, withExpiration, withIndexation);
        return ResponseEntity.ok().body(caseUuid);
    }

    @PostMapping(value = "/cases", params = "duplicateFrom")
    @Operation(summary = "create a case from an existing one")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "The case has been duplicated"),
        @ApiResponse(responseCode = "404", description = "Source case not found"),
        @ApiResponse(responseCode = "500", description = "An error occurred during the case file duplication")})
    public ResponseEntity<UUID> duplicateCase(
            @RequestParam("duplicateFrom") UUID caseId,
            @RequestParam(value = "withExpiration", required = false, defaultValue = "false") boolean withExpiration) {
        LOGGER.debug("duplicateCase request received with parameter sourceCaseUuid = {}", caseId);
        UUID newCaseUuid = caseService.duplicateCase(caseId, withExpiration);
        return ResponseEntity.ok().body(newCaseUuid);
    }

    @PutMapping(value = "/cases/{caseUuid}/disableExpiration")
    @Operation(summary = "disable the case expiration")
    @ApiResponses(value = {
        @ApiResponse(responseCode = "200", description = "The case expiration has been removed"),
        @ApiResponse(responseCode = "404", description = "Source case not found")})
    public ResponseEntity<Void> disableCaseExpiration(@PathVariable("caseUuid") UUID caseUuid) {
        LOGGER.debug("disableCaseExpiration request received for caseUuid = {}", caseUuid);
        metadataService.disableCaseExpiration(caseUuid);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping(value = "/cases/{caseUuid}")
    @Operation(summary = "delete a case")
    public ResponseEntity<Void> deleteCase(@PathVariable("caseUuid") UUID caseUuid) {
        LOGGER.debug("deleteCase request received with parameter caseUuid = {}", caseUuid);
        if (!caseService.caseExists(caseUuid)) {
            throw createDirectoryNotFound(caseUuid);
        }
        caseService.deleteCase(caseUuid);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping(value = "/cases")
    @Operation(summary = "delete all cases")
    public ResponseEntity<Void> deleteCases() {
        LOGGER.debug("deleteCases request received");
        caseService.deleteAllCases();
        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/cases/search")
    @Operation(summary = "Search cases by metadata")
    public ResponseEntity<List<CaseInfos>> searchCases(@RequestParam(value = "q") String query) {
        LOGGER.debug("search cases request received");
        List<CaseInfos> cases = caseInfosService.searchCaseInfos(query);
        return ResponseEntity.ok().body(cases);
    }

    @GetMapping(value = "/cases/metadata")
    @Operation(summary = "Get cases Metadata")
    public ResponseEntity<List<CaseInfos>> getMetadata(@RequestParam("ids") List<UUID> ids) {
        LOGGER.debug("get Cases metadata");
        return ResponseEntity.ok().body(caseService.getMetadata(ids));
    }

    @GetMapping("/cases/caseBaseName")
    @Operation(summary = "Get case base name")
    @ApiResponse(responseCode = "200", description = "case base name retrieved")
    public ResponseEntity<String> getCaseBaseName(@RequestParam("caseName") String caseName) {
        LOGGER.debug("getting base name from case file name");
        String baseName = DataSourceUtil.getBaseName(caseName);
        return ResponseEntity.ok(baseName);
    }
}
