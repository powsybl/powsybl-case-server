/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource.util;

import com.powsybl.caseserver.server.CaseService;
import com.powsybl.caseserver.server.FsCaseService;
import com.powsybl.commons.datasource.DataSource;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = CaseService.class)
public class FsCaseDataSourceService implements CaseDataSourceService {

    @Autowired
    private FsCaseService fsCaseService;

    @Override
    public String getBaseName(UUID caseUuid) {
        DataSource dataSource = getDatasource(caseUuid);
        return dataSource.getBaseName();
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String suffix, String ext) {
        DataSource dataSource = getDatasource(caseUuid);
        try {
            return dataSource.exists(suffix, ext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        DataSource dataSource = getDatasource(caseUuid);
        try {
            return dataSource.exists(fileName);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String fileName) {
        DataSource dataSource = getDatasource(caseUuid);
        try (InputStream inputStream = dataSource.newInputStream(fileName)) {
            return IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String suffix, String ext) {
        DataSource dataSource = getDatasource(caseUuid);
        try (InputStream inputStream = dataSource.newInputStream(suffix, ext)) {
            return IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Set<String> listName(UUID caseUuid, String regex) {
        DataSource dataSource = getDatasource(caseUuid);
        String decodedRegex = URLDecoder.decode(regex, StandardCharsets.UTF_8);
        try {
            return dataSource.listNames(decodedRegex);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DataSource initDatasource(UUID caseUuid) {
        Path file = fsCaseService.getCaseFile(caseUuid);
        return DataSource.fromPath(file);
    }

    private DataSource getDatasource(UUID caseUuid) {
        fsCaseService.checkStorageInitialization();
        return initDatasource(caseUuid);
    }

}

