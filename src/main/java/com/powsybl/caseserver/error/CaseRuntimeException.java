package com.powsybl.caseserver.error;

import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

public class CaseRuntimeException extends RuntimeException {

    public CaseRuntimeException(String message) {
        super(message);
    }

    public CaseRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public static CaseRuntimeException directoryAlreadyExists(String directory) {
        Objects.requireNonNull(directory);
        return new CaseRuntimeException("A directory with the same name already exists: " + directory);
    }

    public static CaseRuntimeException emptyDirectory(Path directory) {
        Objects.requireNonNull(directory);
        return new CaseRuntimeException("The directory is empty: " + directory);
    }

    public static CaseRuntimeException directoryNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("The directory with the following uuid doesn't exist: " + uuid);
    }

    public static CaseRuntimeException originalFileNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("The original file were not retrieved in the directory with the following uuid: " + uuid);
    }

    public static CaseRuntimeException fileNameNotFound(UUID uuid) {
        Objects.requireNonNull(uuid);
        return new CaseRuntimeException("The file name with the following uuid doesn't exist: " + uuid);
    }

    public static CaseRuntimeException storageNotInitialized(Path storageRootDir) {
        Objects.requireNonNull(storageRootDir);
        return new CaseRuntimeException("The storage is not initialized: " + storageRootDir);
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
