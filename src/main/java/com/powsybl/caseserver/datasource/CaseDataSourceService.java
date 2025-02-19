/**
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.datasource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
public interface CaseDataSourceService {
    String getBaseName(UUID caseUuid);

    Boolean datasourceExists(UUID caseUuid, String suffix, String ext);

    Boolean datasourceExists(UUID caseUuid, String fileName);

    InputStream getInputStream(UUID caseUuid, String suffix, String ext) throws IOException;

    InputStream getInputStream(UUID caseUuid, String fileName) throws IOException;

    Set<String> listName(UUID caseUuid, String regex);

}
