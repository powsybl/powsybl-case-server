/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import static com.powsybl.caseserver.CaseException.Type.FILE_NOT_IMPORTABLE;
import static com.powsybl.caseserver.CaseException.Type.ILLEGAL_FILE_NAME;
import static com.powsybl.caseserver.CaseException.Type.STORAGE_DIR_NOT_CREATED;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@ControllerAdvice
public class RestResponseEntityExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestResponseEntityExceptionHandler.class);

    @ExceptionHandler(value = { CaseException.class})
    protected ResponseEntity<Object> handleConflict(RuntimeException ex, WebRequest request) {
        if (LOGGER.isErrorEnabled()) {
            LOGGER.error(ex.getMessage(), ex);
        }
        HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
        if (ex instanceof CaseException caseException) {
            var type = caseException.getType();
            if (type == FILE_NOT_IMPORTABLE || type == ILLEGAL_FILE_NAME || type == STORAGE_DIR_NOT_CREATED) {
                status = HttpStatus.UNPROCESSABLE_ENTITY;
            }
        }
        return handleExceptionInternal(ex, ex.getMessage(), new HttpHeaders(), status, request);
    }
}
