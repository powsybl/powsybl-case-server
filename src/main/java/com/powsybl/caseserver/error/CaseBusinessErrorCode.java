package com.powsybl.caseserver.error;

import com.powsybl.ws.commons.error.BusinessErrorCode;

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
