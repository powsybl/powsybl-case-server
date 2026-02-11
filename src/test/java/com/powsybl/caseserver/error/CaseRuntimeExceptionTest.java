/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.error;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Mohamed Ben-rejeb <mohamed.ben-rejeb at rte-france.com>
 */
class CaseRuntimeExceptionTest {
    @Test
    void directoryNotFound() {
        UUID id = UUID.randomUUID();

        CaseRuntimeException ex =
                CaseRuntimeException.directoryNotFound(id);

        assertTrue(ex.getMessage().contains(id.toString()));
    }

    @Test
    void originalFileNotFound() {
        UUID id = UUID.randomUUID();

        CaseRuntimeException ex =
                CaseRuntimeException.originalFileNotFound(id);

        assertTrue(ex.getMessage().contains(id.toString()));
    }

    @Test
    void tempDirectoryCreation() {
        UUID id = UUID.randomUUID();
        Exception cause = new Exception();

        CaseRuntimeException ex =
                CaseRuntimeException.tempDirectoryCreation(id, cause);

        assertEquals(cause, ex.getCause());
    }

    @Test
    void initTempFile() {
        UUID id = UUID.randomUUID();
        Throwable cause = new RuntimeException();

        CaseRuntimeException ex =
                CaseRuntimeException.initTempFile(id, cause);

        assertEquals(cause, ex.getCause());
    }

    @Test
    void fileNotImportable() {
        Path path = Path.of("file.txt");
        Exception cause = new Exception("err");

        CaseRuntimeException ex =
                CaseRuntimeException.fileNotImportable(path, cause);

        assertTrue(ex.getMessage().contains("file.txt"));
    }
}
