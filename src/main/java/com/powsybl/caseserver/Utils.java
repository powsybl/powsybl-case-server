/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import java.util.List;

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
}
