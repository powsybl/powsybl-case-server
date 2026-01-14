/**
 * Copyright (c) 2019, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import lombok.Getter;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@Getter
public final class CaseException extends RuntimeException {

    public enum Type {
        FILE_NOT_IMPORTABLE,
        FILE_NOT_FOUND,
        STORAGE_DIR_NOT_CREATED,
        ILLEGAL_FILE_NAME,
        DIRECTORY_ALREADY_EXISTS,
        DIRECTORY_EMPTY,
        DIRECTORY_NOT_FOUND,
        ORIGINAL_FILE_NOT_FOUND,
        TEMP_FILE_INIT,
        TEMP_DIRECTORY_CREATION,
        UNPROCESSABLE_CASE_NAME
    }

    private final Type type;

    private CaseException(Type type, String msg) {
        super(msg);
        this.type = Objects.requireNonNull(type);
    }

    public CaseException(Type type, String message, Throwable e) {
        super(message, e);
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public static CaseException createDirectoryAlreadyExists(String directory) {
        Objects.requireNonNull(directory);
        return new CaseException(Type.DIRECTORY_ALREADY_EXISTS, "A directory with the same name already exists: " + directory);
    }

    public static CaseException createDirectoryEmpty(Path directory) {
        Objects.requireNonNull(directory);
        return new CaseException(Type.DIRECTORY_EMPTY, "The directory is empty: " + directory);
    }

    public static CaseException createDirectoryNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseException(Type.DIRECTORY_NOT_FOUND, "The directory with the following uuid doesn't exist: " + uuid);
    }

    public static CaseException createOriginalFileNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseException(Type.ORIGINAL_FILE_NOT_FOUND, "The original file were not retrieved in the directory with the following uuid: " + uuid);
    }

    public static CaseException createFileNotImportable(Path file) {
        Objects.requireNonNull(file);
        return new CaseException(Type.FILE_NOT_IMPORTABLE, "This file cannot be imported: " + file);
    }

    public static CaseException createFileNotImportable(String file, Exception e) {
        Objects.requireNonNull(file);
        return new CaseException(Type.FILE_NOT_IMPORTABLE, "This file cannot be imported: " + file, e);
    }

    public static CaseException createFileNameNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseException(Type.FILE_NOT_FOUND, "The file name with the following uuid doesn't exist: " + uuid);
    }

    public static CaseException createStorageNotInitialized(Path storageRootDir) {
        Objects.requireNonNull(storageRootDir);
        return new CaseException(Type.STORAGE_DIR_NOT_CREATED, "The storage is not initialized: " + storageRootDir);
    }

    public static CaseException createIllegalCaseName(String caseName) {
        Objects.requireNonNull(caseName);
        return new CaseException(Type.ILLEGAL_FILE_NAME, "This is not an acceptable case name: " + caseName);
    }

    public static CaseException createTempDirectory(UUID uuid, Exception e) {
        Objects.requireNonNull(uuid);
        return new CaseException(Type.TEMP_DIRECTORY_CREATION, "Error creating temporary directory: " + uuid, e);
    }

    public static CaseException createInitTempFileError(UUID uuid, Throwable e) {
        Objects.requireNonNull(uuid);
        return new CaseException(Type.TEMP_FILE_INIT, "Error initializing temporary case file: " + uuid, e);
    }
}
