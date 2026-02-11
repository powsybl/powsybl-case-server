/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.error;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Mohamed Ben-rejeb <mohamed.ben-rejeb at rte-france.com>
 */
public class CaseRuntimeException extends RuntimeException {

    public CaseRuntimeException(String message) {
        super(message);
    }

    public CaseRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CaseRuntimeException directoryNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("The directory with the following uuid doesn't exist: " + uuid);
    }

    public static CaseRuntimeException originalFileNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("The original file were not retrieved in the directory with the following uuid: " + uuid);
    }

    public static CaseRuntimeException tempDirectoryCreation(UUID uuid, Exception e) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("Error creating temporary directory: " + uuid, e);
    }

    public static CaseRuntimeException initTempFile(UUID uuid, Throwable e) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("Error initializing temporary case file: " + uuid, e);
    }

    public static CaseRuntimeException fileNotImportable(Path file, Exception e) {
        Objects.requireNonNull(file);
        return new CaseRuntimeException("This file cannot be imported: " + file + " details: " + e.getMessage());
    }
}
