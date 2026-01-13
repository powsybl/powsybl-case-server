/**
 * Copyright (c) 2024, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import com.powsybl.caseserver.service.CaseService;
import com.powsybl.commons.datasource.DataSource;
import com.powsybl.commons.datasource.DataSourceUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.UUID;
import java.util.zip.GZIPInputStream;

import static com.powsybl.caseserver.Utils.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
public class CaseDataSourceService {

    @Autowired
    private CaseService caseService;

    public String getBaseName(UUID caseUuid) {
        return DataSourceUtil.getBaseName(caseService.getCaseName(caseUuid));
    }

    public Boolean datasourceExists(UUID caseUuid, String suffix, String ext) {
        return caseService.datasourceExists(caseUuid, DataSourceUtil.getFileName(getBaseName(caseUuid), suffix, ext));
    }

    public Boolean datasourceExists(UUID caseUuid, String fileName) {
        return caseService.datasourceExists(caseUuid, fileName);
    }

    public InputStream getInputStream(UUID caseUuid, String fileName) {
        String caseName = caseService.getCaseName(caseUuid);
        String caseFileKey;
        // For archived cases (.zip, .tar, ...), individual files are gzipped in S3 server.
        // Here the requested file is decompressed and simply returned.
        if (isArchivedCaseFile(caseName)) {
            caseFileKey = caseService.uuidToKeyWithFileName(caseUuid, fileName + GZIP_EXTENSION);
            return caseService.withS3DownloadedTempPath(caseUuid, caseFileKey,
                    file -> new GZIPInputStream(Files.newInputStream(file)));
        } else {
            if (Boolean.TRUE.equals(caseService.isUploadedAsPlainFile(caseUuid))) {
                caseName += GZIP_EXTENSION;
            }
            caseFileKey = caseService.uuidToKeyWithFileName(caseUuid, caseName);
            return caseService.withS3DownloadedTempPath(caseUuid, caseFileKey,
                    casePath -> DataSource.fromPath(casePath).newInputStream(fileName));
        }
    }

    public InputStream getInputStream(UUID caseUuid, String suffix, String ext) {
        return getInputStream(caseUuid, DataSourceUtil.getFileName(getBaseName(caseUuid), suffix, ext));
    }

    public Set<String> listName(UUID caseUuid, String regex) {
        final String decodedRegex = URLDecoder.decode(regex, StandardCharsets.UTF_8);
        return caseService.listName(caseUuid, decodedRegex);
    }
}
