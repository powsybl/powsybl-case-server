/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.service.S3CaseService;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.commons.datasource.DataSourceUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.function.FailableFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = CaseService.class)
public class S3CaseDataSourceService implements CaseDataSourceService {

    private static final String CASES_PREFIX = "gsi-cases/";

    @Autowired
    private S3CaseService caseService;

    @Override
    public String getBaseName(UUID caseUuid) {
        return DataSourceUtil.getBaseName(caseService.getCaseName(caseUuid));
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String suffix, String ext) {
        return caseService.datasourceExists(caseUuid, DataSourceUtil.getFileName(getBaseName(caseUuid), suffix, ext));
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        return caseService.datasourceExists(caseUuid, fileName);
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String fileName) {
        final var caseFileKey = caseService.getFormat(caseUuid).equals("CGMES")
                ? uuidToPrefixKey(caseUuid) + fileName
                : uuidToPrefixKey(caseUuid) + caseService.getCaseName(caseUuid);
        return withS3DownloadedDataSource(caseUuid, caseFileKey,
            datasource -> IOUtils.toByteArray(datasource.newInputStream(Paths.get(fileName).getFileName().toString())));
    }

    private String uuidToPrefixKey(UUID uuid) {
        return CASES_PREFIX + uuid.toString() + "/";
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String suffix, String ext) {
        return withS3DownloadedDataSource(caseUuid,
            datasource -> IOUtils.toByteArray(datasource.newInputStream(suffix, ext)));
    }

    @Override
    public Set<String> listName(UUID caseUuid, String regex) {
        return caseService.listName(caseUuid, regex);
    }

    public <R, T extends Throwable> R withS3DownloadedDataSource(UUID caseUuid, FailableFunction<DataSource, R, T> f) {
        FailableFunction<Path, DataSource, T> pathToDataSource = DataSource::fromPath;
        FailableFunction<Path, R, T> composedFunction = pathToDataSource.andThen(f);
        return caseService.withS3DownloadedTempPath(caseUuid, composedFunction);
    }

    public <R, T extends Throwable> R withS3DownloadedDataSource(UUID caseUuid, String caseFileKey, FailableFunction<DataSource, R, T> f) {
        FailableFunction<Path, DataSource, T> pathToDataSource = DataSource::fromPath;
        FailableFunction<Path, R, T> composedFunction = pathToDataSource.andThen(f);
        return caseService.withS3DownloadedTempPath(caseUuid, caseFileKey, composedFunction);
    }

}

