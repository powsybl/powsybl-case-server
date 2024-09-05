/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource.util;

import com.powsybl.caseserver.server.CaseService;
import com.powsybl.caseserver.server.S3CaseService;
import com.powsybl.commons.datasource.DataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.function.FailableFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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
        return withS3DownloadedDataSource(caseUuid, DataSource::getBaseName);
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String suffix, String ext) {
        return withS3DownloadedDataSource(caseUuid, datasource -> datasource.exists(suffix, ext));
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        try {
            return withS3DownloadedDataSource(caseUuid, datasource -> datasource.exists(fileName));
        } catch (Exception e) {
            return Boolean.FALSE;
        }
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String fileName) {
        final String parsedFileName = fileName.substring(fileName.indexOf('/') + 1);
        final var caseFileKey = uuidToPrefixKey(caseUuid) + parsedFileName;
        return withS3DownloadedDataSource(caseUuid, caseFileKey,
            datasource -> IOUtils.toByteArray(datasource.newInputStream(parsedFileName)));
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

