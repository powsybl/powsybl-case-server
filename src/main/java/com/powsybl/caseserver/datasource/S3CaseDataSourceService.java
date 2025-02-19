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
import org.apache.commons.compress.utils.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Set;
import java.util.UUID;

import static com.powsybl.caseserver.Utils.*;

/**
 * @author Ghazwa Rehili <ghazwa.rehili at rte-france.com>
 * @author Etienne Homer <etienne.homer at rte-france.com>
 */
@Service
@ComponentScan(basePackageClasses = CaseService.class)
public class S3CaseDataSourceService implements CaseDataSourceService {

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
    public InputStream getInputStream(UUID caseUuid, String fileName) {
        //FIXME: to do for S3
        return null;
    }

    @Override
    public InputStream getInputStream(UUID caseUuid, String suffix, String ext) {
        return getInputStream(caseUuid, DataSourceUtil.getFileName(getBaseName(caseUuid), suffix, ext));
    }

    @Override
    public Set<String> listName(UUID caseUuid, String regex) {
        return s3CaseService.listName(caseUuid, regex);
    }
}

