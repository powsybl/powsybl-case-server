/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.caseserver.datasource.utils;

import com.powsybl.caseserver.service.CaseService;
import org.springframework.web.multipart.MultipartFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.UUID;

/**
 * @author Bassel El Cheikh <bassel.el-cheikh_externe at rte-france.com>
 */

public class S3MultiPartFile implements MultipartFile, Closeable {

    private final String name;
    private final String contentType;
    private final CaseService caseService;
    private final UUID caseUuid;
    private final String folderName;
    private Path tempFile;
    private long size;

    public S3MultiPartFile(CaseService caseService, UUID caseUuid, String folderName, String name, String contentType) throws IOException {
        this.name = name != null ? name : "";
        this.contentType = contentType;
        this.caseService = caseService;
        this.caseUuid = caseUuid;
        this.folderName = folderName;
        init();
    }

    private void init() throws IOException {
        FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
        this.tempFile = Files.createTempFile("s3-create-case-", caseUuid.toString(), attr);
        try (InputStream in = caseService.getInputStreamFromS3(caseUuid, folderName, name)
            .orElseThrow(() -> new IOException("Could not retrieve file from S3: " + name))) {
            Files.copy(in, this.tempFile, StandardCopyOption.REPLACE_EXISTING);
        }
        this.size = Files.size(this.tempFile);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getOriginalFilename() {
        return getName();
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public boolean isEmpty() {
        return getSize() == 0;
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public byte[] getBytes() throws IOException {
        return Files.readAllBytes(tempFile);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return Files.newInputStream(tempFile);
    }

    @Override
    public void transferTo(File dest) throws IOException, IllegalStateException {
        Files.copy(tempFile, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public void close() throws IOException {
        if (tempFile != null) {
            Files.deleteIfExists(tempFile);
            tempFile = null;
        }
    }
}
