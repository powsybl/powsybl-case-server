/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Etienne Lesot <etienne.lesot at rte-france.com>
 */
public final class Utils {

    private Utils() {

    }

    public static final String GZIP_EXTENSION = ".gz";
    public static final String GZIP_FORMAT = "gz";
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

    public static byte[] decompress(byte[] data) throws IOException {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(data));
        return IOUtils.toByteArray(gzipInputStream);
    }

    public static byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(data, 0, data.length);
        gzipOutputStream.close();
        return outputStream.toByteArray();
    }

    public static FileAttribute<Set<PosixFilePermission>> getRwxAttribute() {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"));
    }
}
