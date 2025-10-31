/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.elasticsearch.DisableElasticsearch;
import com.powsybl.caseserver.parsers.FileNameInfos;
import com.powsybl.caseserver.parsers.FileNameParser;
import com.powsybl.caseserver.parsers.entsoe.EntsoeFileNameParser;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisableElasticsearch
@Import(DisableElasticsearch.MockConfig.class)
class CaseFileNameParserTests {
    private static final String CASE_FILE_NAME_INCORRECT = "20200103_0915_SN5.UCT";
    private static final String TEST_OTHER_CASE_FILE_NAME = "testCase.xiidm";

    @Test
    void testFileNameIncorrect() {
        Path casePath = Path.of(this.getClass().getResource("/" + CASE_FILE_NAME_INCORRECT).getPath());
        String fileBaseName = casePath.getFileName().toString();
        FileNameParser parser = new EntsoeFileNameParser();
        Optional<? extends FileNameInfos> fileNameInfos = parser.parse(fileBaseName);
        assertTrue(fileNameInfos.isEmpty());
    }

    @Test
    void testFileNameParserIncorrect() {
        final FileNameParser parserIncorrect = fileBaseName -> false;
        assertThrows(UnsupportedOperationException.class, () -> parserIncorrect.parse(TEST_OTHER_CASE_FILE_NAME));
    }
}
