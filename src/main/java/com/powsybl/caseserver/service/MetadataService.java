/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.repository.CaseMetadataEntity;
import com.powsybl.caseserver.repository.CaseMetadataRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.UUID;

import static com.powsybl.caseserver.Utils.NOT_FOUND;

/**
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
public class MetadataService {

    private final CaseMetadataRepository caseMetadataRepository;

    public MetadataService(CaseMetadataRepository caseMetadataRepository) {
        this.caseMetadataRepository = caseMetadataRepository;
    }

    @Transactional
    public void disableCaseExpiration(UUID caseUuid) {
        CaseMetadataEntity caseMetadataEntity = caseMetadataRepository.findById(caseUuid).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Metadata of case " + caseUuid + NOT_FOUND));
        caseMetadataEntity.setExpirationDate(null);
    }
}
