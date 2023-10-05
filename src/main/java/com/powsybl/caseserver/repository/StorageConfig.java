/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.repository;

import com.powsybl.caseserver.CaseService;
import com.powsybl.caseserver.FileSystemStorageService;
import com.powsybl.caseserver.ObjectStorageService;
import com.powsybl.caseserver.datasource.util.CaseDataSourceService;
import com.powsybl.caseserver.datasource.util.FsCaseDataSourceService;
import com.powsybl.caseserver.datasource.util.S3CaseDataSourceService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;

@Configuration
public class StorageConfig {

    private final CaseMetadataRepository caseMetadataRepository;
    private final String storageType;

    public StorageConfig(@Value("${storage.type}")String storageType, CaseMetadataRepository caseMetadataRepository) {
        this.storageType = storageType;
        this.caseMetadataRepository = caseMetadataRepository;
    }

    @Primary
    @Bean
    public CaseService storageService() {
        if ("file".equals(storageType)) {
            return new FileSystemStorageService(caseMetadataRepository);
        } else if ("S3".equals(storageType)) {
            return new ObjectStorageService(caseMetadataRepository);
        }
        return null;
    }

    @Primary
    @Bean
    public CaseDataSourceService caseSourceService() {
        if ("file".equals(storageType)) {
            return new FsCaseDataSourceService();
        } else if ("S3".equals(storageType)) {
            return new S3CaseDataSourceService();
        }
        return null;
    }
}
