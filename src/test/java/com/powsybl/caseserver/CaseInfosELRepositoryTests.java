/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver;

import com.powsybl.caseserver.service.CaseService;
import com.powsybl.caseserver.dto.CaseInfos;
import com.powsybl.caseserver.dto.cgmes.CgmesCaseInfos;
import com.powsybl.caseserver.dto.entsoe.EntsoeCaseInfos;
import com.powsybl.caseserver.elasticsearch.CaseInfosService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class CaseInfosELRepositoryTests {
    private static final String SN_UCTE_CASE_FILE_NAME = "20200103_0915_SN5_D80.UCT";
    private static final String ID1_UCTE_CASE_FILE_NAME = "20200103_0915_135_CH2.UCT";
    private static final String ID2_UCTE_CASE_FILE_NAME = "20200424_1330_135_CH2.UCT";
    private static final String FO1_UCTE_CASE_FILE_NAME = "20200103_0915_FO5_FR0.UCT";
    private static final String FO2_UCTE_CASE_FILE_NAME = "20200110_0430_FO5_FR0.uct";
    private static final String D4_UCTE_CASE_FILE_NAME = "20200430_1530_2D4_D41.uct";
    private static final String TEST_CGMES_CASE_FILE_NAME = "20200424T1330Z_2D_RTEFRANCE_001.zip";

    private static final String UCTE_FORMAT = "UCTE";
    private static final String CGMES_FORMAT = "CGMES";

    @Autowired
    private CaseService caseService;

    @Autowired
    private CaseInfosService caseInfosService;

    @Test
    void testAddDeleteCaseInfos() {
        EntsoeCaseInfos caseInfos1 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(SN_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        Optional<CaseInfos> caseInfosAfter1 = caseInfosService.getCaseInfosByUuid(caseInfos1.getUuid().toString());
        assertFalse(caseInfosAfter1.isEmpty());
        assertEquals(caseInfos1, caseInfosAfter1.get());
        assertThat(caseInfosAfter1.get()).usingRecursiveAssertion().isEqualTo(caseInfos1);

        EntsoeCaseInfos caseInfos2 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(ID2_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        Optional<CaseInfos> caseInfosAfter2 = caseInfosService.getCaseInfosByUuid(caseInfos2.getUuid().toString());
        assertFalse(caseInfosAfter2.isEmpty());
        assertEquals(caseInfos2, caseInfosAfter2.get());
        assertThat(caseInfosAfter2.get()).usingRecursiveAssertion().isEqualTo(caseInfos2);

        caseInfosService.deleteCaseInfosByUuid(caseInfos1.getUuid().toString());
        caseInfosAfter1 = caseInfosService.getCaseInfosByUuid(caseInfos1.getUuid().toString());
        assertTrue(caseInfosAfter1.isEmpty());

        caseInfosService.deleteCaseInfos(caseInfos2);
        caseInfosAfter2 = caseInfosService.getCaseInfosByUuid(caseInfos2.getUuid().toString());
        assertTrue(caseInfosAfter2.isEmpty());

        caseInfosService.addCaseInfos(caseInfos1);
        caseInfosService.addCaseInfos(caseInfos2);
        List<CaseInfos> all = caseInfosService.getAllCaseInfos();
        assertFalse(all.isEmpty());
        caseInfosService.deleteAllCaseInfos();
        all = caseInfosService.getAllCaseInfos();
        assertTrue(all.isEmpty());

        caseInfosService.recreateAllCaseInfos(List.of(createInfos(SN_UCTE_CASE_FILE_NAME, UCTE_FORMAT), createInfos(TEST_CGMES_CASE_FILE_NAME, CGMES_FORMAT)));
        all = caseInfosService.getAllCaseInfos();
        assertEquals(2, all.size());
        assertEquals(SN_UCTE_CASE_FILE_NAME, all.get(0).getName());
        assertEquals("UCTE", all.get(0).getFormat());
        assertEquals(TEST_CGMES_CASE_FILE_NAME, all.get(1).getName());
        assertEquals("CGMES", all.get(1).getFormat());
    }

    @Test
    void searchCaseInfos() {
        caseInfosService.deleteAllCaseInfos();
        List<CaseInfos> all = caseInfosService.getAllCaseInfos();
        assertTrue(all.isEmpty());

        EntsoeCaseInfos ucte1 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(SN_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        EntsoeCaseInfos ucte2 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(ID1_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        EntsoeCaseInfos ucte3 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(ID2_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        EntsoeCaseInfos ucte4 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(FO1_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        EntsoeCaseInfos ucte5 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(FO2_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        EntsoeCaseInfos ucte6 = (EntsoeCaseInfos) caseInfosService.addCaseInfos(createInfos(D4_UCTE_CASE_FILE_NAME, UCTE_FORMAT));
        CgmesCaseInfos cgmes = (CgmesCaseInfos) caseInfosService.addCaseInfos(createInfos(TEST_CGMES_CASE_FILE_NAME, CGMES_FORMAT));

        all = caseInfosService.searchCaseInfos("*");
        assertFalse(all.isEmpty());
        assertTrue(all.contains(ucte1));
        assertTrue(all.contains(ucte2));
        assertTrue(all.contains(ucte3));
        assertTrue(all.contains(ucte4));
        assertTrue(all.contains(ucte5));
        assertTrue(all.contains(ucte6));
        assertTrue(all.contains(cgmes));

        List<CaseInfos> list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte1.getDate()));
        assertTrue(list.size() == 3 && list.contains(ucte1) && list.contains(ucte2) && list.contains(ucte4));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte3.getDate()));
        assertTrue(list.size() == 1 && list.contains(ucte3));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte5.getDate()));
        assertTrue(list.size() == 1 && list.contains(ucte5));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte6.getDate()));
        assertTrue(list.size() == 1 && list.contains(ucte6));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(cgmes.getDate()));
        assertTrue(list.size() == 1 && list.contains(cgmes));

        list = caseInfosService.searchCaseInfos("geographicalCode:(D8)");
        assertTrue(list.size() == 1 && list.contains(ucte1));
        list = caseInfosService.searchCaseInfos("geographicalCode:(CH)");
        assertTrue(list.size() == 2 && list.contains(ucte2) && list.contains(ucte3));
        list = caseInfosService.searchCaseInfos("geographicalCode:(FR)");
        assertTrue(list.size() == 2 && list.contains(ucte4) && list.contains(ucte5));
        list = caseInfosService.searchCaseInfos("geographicalCode:(D4)");
        assertTrue(list.size() == 1 && list.contains(ucte6));
        list = caseInfosService.searchCaseInfos("tso:(RTEFRANCE)");
        assertTrue(list.size() == 1 && list.contains(cgmes));

        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte1.getDate()) + " AND geographicalCode:(D8)");
        assertTrue(list.size() == 1 && list.contains(ucte1));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte3.getDate()) + " AND geographicalCode:(CH)");
        assertTrue(list.size() == 1 && list.contains(ucte3));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte5.getDate()) + " AND geographicalCode:(FR)");
        assertTrue(list.size() == 1 && list.contains(ucte5));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte6.getDate()) + " AND geographicalCode:(D4)");
        assertTrue(list.size() == 1 && list.contains(ucte6));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(cgmes.getDate()) + " AND tso:(RTEFRANCE) AND businessProcess:(2D)");
        assertTrue(list.size() == 1 && list.contains(cgmes));

        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte4.getDate()) + " AND geographicalCode:(FR OR CH OR D8)");
        assertTrue(list.size() == 3 && list.contains(ucte1) && list.contains(ucte2) && list.contains(ucte4));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte4.getDate()) + " AND geographicalCode:(FR OR CH)");
        assertTrue(list.size() == 2 && list.contains(ucte2) && list.contains(ucte4));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte4.getDate()) + " AND geographicalCode:(FR OR D8)");
        assertTrue(list.size() == 2 && list.contains(ucte1) && list.contains(ucte4));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte4.getDate()) + " AND geographicalCode:(CH OR D8)");
        assertTrue(list.size() == 2 && list.contains(ucte1) && list.contains(ucte2));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(cgmes.getDate()) + " AND tso:(REE OR RTEFRANCE OR REN)");
        assertTrue(list.size() == 1 && list.contains(cgmes));
        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(cgmes.getDate()) + " AND tso:(REE OR RTEFRANCE OR REN) AND businessProcess:(2D) AND format:CGMES");
        assertTrue(list.size() == 1 && list.contains(cgmes));

        list = caseInfosService.searchCaseInfos("geographicalCode:(D4 OR D8)");
        assertTrue(list.size() == 2 && list.contains(ucte1) && list.contains(ucte6));

        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte6.getDate()) + " OR " + CaseInfosService.getDateSearchTerm(cgmes.getDate()));
        assertTrue(list.size() == 2 && list.contains(ucte6) && list.contains(cgmes));

        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte6.getDate(), cgmes.getDate()));
        assertTrue(list.size() == 2 && list.contains(ucte6) && list.contains(cgmes));

        list = caseInfosService.searchCaseInfos(CaseInfosService.getDateSearchTerm(ucte1.getDate()) + " AND geographicalCode:D8 AND forecastDistance:0");
        assertTrue(list.size() == 1 && list.contains(ucte1));
    }

    private CaseInfos createInfos(String fileName, String format) {
        Path casePath = Path.of(this.getClass().getResource("/" + fileName).getPath());
        String fileBaseName = casePath.getFileName().toString();
        return caseService.createInfos(fileBaseName, UUID.randomUUID(), format);
    }
}
