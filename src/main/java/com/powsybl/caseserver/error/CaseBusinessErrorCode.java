/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.error;

import com.powsybl.ws.commons.error.BusinessErrorCode;

/**
 * @author Mohamed Ben-rejeb <mohamed.ben-rejeb at rte-france.com>
 */
public enum CaseBusinessErrorCode implements BusinessErrorCode {
    NO_AVAILABLE_IMPORTER("case.noAvailableImporter"),
    ILLEGAL_FILE_NAME("case.illegalFileName");
    private final String code;
    CaseBusinessErrorCode(String code) {
        this.code = code;
    }

    public String value() {
        return code;
    }
}
