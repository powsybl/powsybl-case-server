/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.migration;

import com.powsybl.caseserver.CaseConstants;
import com.powsybl.caseserver.CaseController;
import com.powsybl.caseserver.service.CaseService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

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
    private S3MigrationCaseService migrationCaseService;

    @Autowired
    @Qualifier("storageService")
    private CaseService caseService;

//    @PutMapping(value = "/toS3/{caseUuid}")
//    @Operation(summary = "Migrate limits of a network")
//    @ApiResponses(@ApiResponse(responseCode = "200", description = "Case successfully migrated from FS to S3"))
//    public ResponseEntity<Void> migrateV211Limits(@Parameter(description = "Case uuid", required = true) @PathVariable("caseUuid") UUID caseUuid) throws Exception {
//        migrationCaseService.migrateCase(caseUuid);
//        return ResponseEntity.ok().build();
//    }

    @PostMapping(value = "/cases", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    @Operation(summary = "import a case")
    @SuppressWarnings("javasecurity:S5145")
    public ResponseEntity<UUID> importCase(@RequestParam("file") MultipartFile file,
                                           @RequestParam(value = "withExpiration", required = false, defaultValue = "false") boolean withExpiration,
                                           @RequestParam(value = "withIndexation", required = false, defaultValue = "false") boolean withIndexation,
                                           @RequestParam(value = "caseUuid") UUID caseUuid) {
        LOGGER.debug("importCase request received with file = {}", file.getName());
        caseService.importCase(file, withExpiration, withIndexation, caseUuid);
        return ResponseEntity.ok().body(caseUuid);
    }
}
