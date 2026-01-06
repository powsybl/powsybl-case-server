/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.commons.datasource.DataSourceUtil;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
public final class Utils {

    private Utils() {

    }

    public static final String GZIP_EXTENSION = ".gz";
    public static final String GZIP_FORMAT = "gz";
    public static final String GZIP_ENCODING = "gzip";
    public static final List<String> COMPRESSION_FORMATS = List.of("bz2", GZIP_FORMAT, "xz", "zst");
    public static final List<String> ARCHIVE_FORMATS = List.of("zip", "tar");
    public static final String NOT_FOUND = " not found";

    public static String removeExtension(String filename, String extension) {
        int index = filename.lastIndexOf(extension);
        if (index == -1 || index < filename.length() - extension.length() /*extension to remove is not at the end*/) {
            return filename;
        }
        return filename.substring(0, index);
    }

    public static boolean isCompressedCaseFile(String caseName) {
        return COMPRESSION_FORMATS.stream().anyMatch(cf -> caseName.endsWith("." + cf));
    }

    public static boolean isArchivedCaseFile(String caseName) {
        return ARCHIVE_FORMATS.stream().anyMatch(cf -> caseName.endsWith("." + cf));
    }

    public static boolean isZippedFile(String caseName) {
        return caseName.endsWith(".zip");
    }

    public static boolean isTaredFile(String caseName) {
        return caseName.endsWith(".tar");
    }

    public static HttpHeaders buildHeaders(String name, Boolean isUploadedAsPlainFile) {
        String baseName = DataSourceUtil.getBaseName(name);
        String extension = name.replaceFirst(Pattern.quote(baseName) + ".", "");
        HttpHeaders headers = new HttpHeaders();
        headers.add("extension", extension);
        String contentType;
        // For plain files, we return the gzip file as it stored in S3. And we rely on content encoding to let the browser decompress the file.
        if (Boolean.TRUE.equals(isUploadedAsPlainFile)) {
            headers.add(HttpHeaders.CONTENT_ENCODING, GZIP_ENCODING);
            contentType = MediaType.TEXT_PLAIN.toString();
        } else {
            try {
                contentType = Files.probeContentType(Path.of(name));
            } catch (IOException e) {
                throw CaseException.createIllegalCaseName(name);
            }

        }
        headers.add(HttpHeaders.CONTENT_TYPE, contentType);
        return headers;
    }
}
