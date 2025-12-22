/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.error;

import com.powsybl.caseserver.PropertyServerNameProvider;
import com.powsybl.ws.commons.error.AbstractBusinessExceptionHandler;
import com.powsybl.ws.commons.error.PowsyblWsProblemDetail;
import jakarta.servlet.http.HttpServletRequest;
import lombok.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@ControllerAdvice
public class CaseExceptionHandler extends AbstractBusinessExceptionHandler<CaseBusinessException, CaseBusinessErrorCode> {
    protected CaseExceptionHandler(PropertyServerNameProvider serverNameProvider) {
        super(serverNameProvider);
    }

    @Override
    protected @NonNull CaseBusinessErrorCode getBusinessCode(CaseBusinessException e) {
        return e.getBusinessErrorCode();
    }

    @ExceptionHandler(CaseBusinessException.class)
    protected ResponseEntity<PowsyblWsProblemDetail> handleCaseBusinessException(
        CaseBusinessException exception, HttpServletRequest request) {
        return super.handleDomainException(exception, request);
    }

    @Override
    protected HttpStatus mapStatus(CaseBusinessErrorCode errorCode) {
        return switch (errorCode) {
            case NO_AVAILABLE_IMPORTER, ILLEGAL_FILE_NAME -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
    }
}
