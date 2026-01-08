/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.caseserver.datasource.utils;

import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * @author Bassel El Cheikh <bassel.el-cheikh_externe at rte-france.com>
 */

public class InputStreamMultiPartFile implements MultipartFile {

    private final String name;
    private final byte[] contentBytes; // Store file content in bytes
    private final String contentType;

    public InputStreamMultiPartFile(InputStream inputStream, String name, String contentType) throws IOException {
        this.name = name != null ? name : "";
        this.contentType = contentType;

        // Read all bytes at once and store them for later use
        this.contentBytes = inputStream.readAllBytes();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getOriginalFilename() {
        return name;
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public boolean isEmpty() {
        return contentBytes.length == 0;
    }

    @Override
    public long getSize() {
        return contentBytes.length;
    }

    @Override
    public byte[] getBytes() {
        return contentBytes;
    }

    @Override
    public InputStream getInputStream() {
        return new ByteArrayInputStream(contentBytes);
    }

    @Override
    public void transferTo(File dest) throws IOException, IllegalStateException {
        try (InputStream in = getInputStream()) {
            Files.copy(in, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
