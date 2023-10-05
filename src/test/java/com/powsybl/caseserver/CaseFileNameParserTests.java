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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DisableElasticsearch
public class CaseFileNameParserTests {

    private static final String CASE_FILE_NAME_INCORRECT = "20200103_0915_SN5.UCT";
    private static final String TEST_OTHER_CASE_FILE_NAME = "testCase.xiidm";

    @Test
    public void testFileNameIncorrect() {
        Path casePath = Path.of(this.getClass().getResource("/" + CASE_FILE_NAME_INCORRECT).getPath());
        String fileBaseName = casePath.getFileName().toString();
        FileNameParser parser = new EntsoeFileNameParser();
        Optional<? extends FileNameInfos> fileNameInfos = parser.parse(fileBaseName);
        assertTrue(fileNameInfos.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testFileNameParserIncorrect() {
        FileNameParser parserIncorrect = fileBaseName -> false;
        parserIncorrect.parse(TEST_OTHER_CASE_FILE_NAME);
    }
}
