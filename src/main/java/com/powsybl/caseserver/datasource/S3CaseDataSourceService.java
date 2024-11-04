/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.util.Set;
import java.util.UUID;

import static com.powsybl.caseserver.service.S3CaseService.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = CaseService.class)
public class S3CaseDataSourceService implements CaseDataSourceService {

    private static final String CASES_PREFIX = "gsi-cases/";

    @Autowired
    private S3CaseService s3CaseService;

    @Override
    public String getBaseName(UUID caseUuid) {
        return DataSourceUtil.getBaseName(s3CaseService.getCaseName(caseUuid));
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String suffix, String ext) {
        return s3CaseService.datasourceExists(caseUuid, DataSourceUtil.getFileName(getBaseName(caseUuid), suffix, ext));
    }

    @Override
    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        return s3CaseService.datasourceExists(caseUuid, fileName);
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String fileName) {
        String caseName = s3CaseService.getCaseName(caseUuid);
        String caseFileKey;
        // For archived cases (.zip, .tar, ...), individual files are gzipped in S3 server.
        // Here the requested file is decompressed and simply returned.
        if (S3CaseService.isArchivedCaseFile(caseName)) {
            caseFileKey = uuidToKeyWithFileName(caseUuid, fileName + GZIP_EXTENSION);
            return s3CaseService.withS3DownloadedTempPath(caseUuid, caseFileKey,
                    file -> S3CaseService.decompress(Files.readAllBytes(file)));
        } else {
            caseFileKey = uuidToKeyWithFileName(caseUuid, caseName);
            return s3CaseService.withS3DownloadedTempPath(caseUuid, caseFileKey,
                    casePath -> IOUtils.toByteArray(DataSource.fromPath(casePath).newInputStream(fileName)));
        }
    }

    @Override
    public byte[] getInputStream(UUID caseUuid, String suffix, String ext) {
        return getInputStream(caseUuid, DataSourceUtil.getFileName(getBaseName(caseUuid), suffix, ext));
    }

    @Override
    public Set<String> listName(UUID caseUuid, String regex) {
        return s3CaseService.listName(caseUuid, regex);
    }
}

