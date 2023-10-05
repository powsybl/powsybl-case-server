/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource.util;

import com.powsybl.caseserver.CaseService;
import com.powsybl.commons.datasource.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Functions.FailableFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = CaseService.class)
public class S3CaseDataSourceService implements CaseDataSourceService {

    @Autowired
    @Qualifier("objectStorageService")
    private CaseService caseService;

    public String getBaseName(UUID caseUuid) {
        return withS3DownloadedDataSource(caseUuid, DataSource::getBaseName);
    }

    public Boolean datasourceExists(UUID caseUuid, String suffix, String ext) {
        return withS3DownloadedDataSource(caseUuid, datasource -> datasource.exists(suffix, ext));
    }

    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        try {
            return withS3DownloadedDataSource(caseUuid, datasource -> datasource.exists(fileName));
        } catch (Exception e) {
            return Boolean.FALSE;
        }
    }

    public byte[] getInputStream(UUID caseUuid, String fileName) {
        return withS3DownloadedDataSource(caseUuid,
            datasource -> IOUtils.toByteArray(datasource.newInputStream(fileName)));
    }

    public byte[] getInputStream(UUID caseUuid, String suffix, String ext) {
        return withS3DownloadedDataSource(caseUuid,
            datasource -> IOUtils.toByteArray(datasource.newInputStream(suffix, ext)));
    }

    public Set<String> listName(UUID caseUuid, String regex) {
        return withS3DownloadedDataSource(caseUuid, datasource -> {
            String decodedRegex = URLDecoder.decode(regex, StandardCharsets.UTF_8);
            try {
                return datasource.listNames(decodedRegex);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public <R, T extends Throwable> R withS3DownloadedDataSource(UUID caseUuid, FailableFunction<DataSource, R, T> f) {
        // TODO replace with lang3 3.11 FailableFunction::compose
        return caseService.withS3DownloadedTempPath(caseUuid, path -> f.apply(DataSource.fromPath(path)));
    }

}

