/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.caseserver.datasource.utils;

import com.powsybl.caseserver.service.CaseService;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * @author Bassel El Cheikh <bassel.el-cheikh_externe at rte-france.com>
 */

public class S3MultiPartFile implements MultipartFile {

    private final String name;
    private final String contentType;
    private final CaseService caseService;
    private final UUID caseUuid;
    private final String folderName;

    public S3MultiPartFile(CaseService caseService, UUID caseUuid, String folderName, String name, String contentType) {
        this.name = name != null ? name : "";
        this.contentType = contentType;
        this.caseService = caseService;
        this.caseUuid = caseUuid;
        this.folderName = folderName;
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
        try (InputStream in = getInputStream()) {
            return in.available() == 0;
        } catch (IOException e) {
            return true;
        }
    }

    @Override
    public long getSize() {
        try (InputStream in = getInputStream()) {
            return in.available();
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public byte[] getBytes() throws IOException {
        return getInputStream().readAllBytes();
    }

    @Override
    public InputStream getInputStream() {
        return caseService.getInputStreamFromS3(caseUuid, folderName, name).orElse(InputStream.nullInputStream());
    }

    @Override
    public void transferTo(File dest) throws IOException, IllegalStateException {
        try (InputStream in = getInputStream()) {
            Files.copy(in, dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
