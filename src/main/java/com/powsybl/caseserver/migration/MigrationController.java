/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.migration;

import com.powsybl.caseserver.CaseConstants;
import com.powsybl.caseserver.service.CaseService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@RestController
@RequestMapping(value = "/" + CaseConstants.API_VERSION + "/migration")
@Tag(name = "Case migration")
public class MigrationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationController.class);

    @Autowired
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
}
