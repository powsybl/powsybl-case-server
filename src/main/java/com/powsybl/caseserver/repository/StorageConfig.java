/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.repository;

import com.powsybl.caseserver.datasource.CaseDataSourceService;
import com.powsybl.caseserver.datasource.FsCaseDataSourceService;
import com.powsybl.caseserver.datasource.S3CaseDataSourceService;
import com.powsybl.caseserver.service.CaseObserver;
import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.service.CaseService.StorageType;
import com.powsybl.caseserver.service.FsCaseService;
import com.powsybl.caseserver.service.S3CaseService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@Configuration
public class StorageConfig {

    private final CaseMetadataRepository caseMetadataRepository;
    private final CaseObserver caseObserver;
    private final StorageType storageType;

    public StorageConfig(@Value("${storage.type}") StorageType storageType, CaseMetadataRepository caseMetadataRepository, CaseObserver caseObserver) {
        this.storageType = storageType;
        this.caseMetadataRepository = caseMetadataRepository;
        this.caseObserver = caseObserver;
    }

    @Primary
    @Bean
    public CaseService storageService() {
        if (StorageType.FS == storageType) {
            return new FsCaseService(caseMetadataRepository, caseObserver);
        } else if (StorageType.S3 == storageType) {
            return new S3CaseService(caseMetadataRepository, caseObserver);
        }
        return null;
    }

    @Primary
    @Bean
    public CaseDataSourceService caseDataSourceService() {
        if (StorageType.FS == storageType) {
            return new FsCaseDataSourceService();
        } else if (StorageType.S3 == storageType) {
            return new S3CaseDataSourceService();
        }
        return null;
    }
}
