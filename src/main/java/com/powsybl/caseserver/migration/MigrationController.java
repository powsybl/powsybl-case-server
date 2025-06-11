/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.migration;

import com.powsybl.caseserver.CaseConstants;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import com.powsybl.caseserver.service.CaseService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@RestController
@RequestMapping(value = "/" + CaseConstants.API_VERSION + "/migration")
@Tag(name = "Case migration")
public class MigrationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationController.class);

    @Autowired
    @Qualifier("storageService")
    private CaseService caseService;

    @PostMapping(value = "/cases", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "import a case with given uuid")
    @SuppressWarnings("javasecurity:S5145")
    public ResponseEntity<UUID> importCase(@RequestParam("file") MultipartFile file,
                                           @RequestParam(value = "withExpiration", defaultValue = "false") boolean withExpiration,
                                           @RequestParam(value = "withIndexation", defaultValue = "false") boolean withIndexation,
                                           @RequestParam(value = "caseUuid") UUID caseUuid) {
        LOGGER.debug("importCase request received with file = {}", file.getOriginalFilename());
        if (caseService.caseExists(caseUuid)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Case with UUID " + caseUuid + " already exists");
        }
        caseService.importCase(file, withExpiration, withIndexation, caseUuid);
        return ResponseEntity.ok().build();
    }

    @PutMapping(value = "/cases/{caseUuid}/metadata")
    @Operation(summary = "complete case metadata")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Update case metadata")})
    @Transactional
    public ResponseEntity<Void> completeCasesMetadata(@PathVariable("caseUuid") UUID caseUuid) {
        CaseMetadataRepository repository = caseService.getCaseMetadataRepository();
        Optional<CaseMetadataEntity> entity = repository.findById(caseUuid);
        if (entity.isPresent()) {
            CaseInfos info = caseService.getCaseInfos(caseUuid);
            entity.get().setOriginalFilename(info.getName());
            entity.get().setFormat(info.getFormat());
        }
        return ResponseEntity.ok().build();
    }

    @GetMapping(value = "/cases/emptyMetadata")
    @Operation(summary = "get cases ids with empty metadata")
    @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "get cases ids with empty metadata")})
    public ResponseEntity<List<UUID>> getCasesWithNoMetaData() {
        CaseMetadataRepository repository = caseService.getCaseMetadataRepository();
        List<CaseMetadataEntity> entities = repository.getCaseMetadataEntitiesByOriginalFilename(null);

        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(entities.stream().map(entity -> entity.getId()).toList());
    }
}
