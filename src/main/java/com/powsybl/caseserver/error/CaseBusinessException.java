/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.error;

import com.powsybl.ws.commons.error.AbstractBusinessException;
import lombok.Getter;
import lombok.NonNull;

import java.nio.file.Path;
import java.util.Objects;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 * @author Mohamed Ben-rejeb <mohamed.ben-rejeb at rte-france.com>
 */
@Getter
public final class CaseBusinessException extends AbstractBusinessException {

    private final CaseBusinessErrorCode errorCode;

    private CaseBusinessException(CaseBusinessErrorCode errorCode, String message) {
        super(Objects.requireNonNull(message, "message must not be null"));
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode must not be null");
    }

    public static CaseBusinessException createIllegalCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        return new CaseBusinessException(CaseBusinessErrorCode.ILLEGAL_FILE_NAME, "This is not an acceptable case name: " + caseName);
    }

    public static CaseBusinessException noAvailableImporter(Path file) {
        Objects.requireNonNull(file);
        return new CaseBusinessException(CaseBusinessErrorCode.NO_AVAILABLE_IMPORTER, "No available importer found for this file: " + file);
    }

    @NonNull
    @Override
    public CaseBusinessErrorCode getBusinessErrorCode() {
        return errorCode;
    }
}
