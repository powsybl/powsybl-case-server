/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;

import com.powsybl.caseserver.service.CaseService.StorageType;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.NonNull;
import org.springframework.stereotype.Service;

/**
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
@Service
public class CaseObserver {
    private static final String OBSERVATION_PREFIX = "app.case.";
    private static final String STORAGE_TYPE_TAG_NAME = "storage_type";

    private static final String CASE_WRITING_OBSERVATION_NAME = OBSERVATION_PREFIX + "writing";
    private static final String CASE_IMPORT_OBSERVATION_NAME = OBSERVATION_PREFIX + "import";
    private static final String CASE_SIZE_METER_NAME = CASE_IMPORT_OBSERVATION_NAME + ".size";

    private static final String CASE_EXIST_OBSERVATION_NAME = OBSERVATION_PREFIX + "is_exist";

    private final ObservationRegistry observationRegistry;
    private final MeterRegistry meterRegistry;

    public CaseObserver(@NonNull ObservationRegistry observationRegistry, @NonNull MeterRegistry meterRegistry) {
        this.observationRegistry = observationRegistry;
        this.meterRegistry = meterRegistry;
    }

    public <E extends Throwable> void observeCaseWriting(StorageType storageType, Observation.CheckedRunnable<E> runnable) throws E {
        createObservation(CASE_WRITING_OBSERVATION_NAME, storageType).observeChecked(runnable);
    }

    public <E extends Throwable> void observeCaseImport(long caseSize, StorageType storageType, Observation.CheckedRunnable<E> runnable) throws E {
        createObservation(CASE_IMPORT_OBSERVATION_NAME, storageType).observeChecked(runnable);
        recordCaseSize(caseSize, storageType);
    }

    public <E extends Throwable> Boolean observeCaseExist(StorageType storageType, Observation.CheckedCallable<Boolean, E> callable) throws E {
        return createObservation(CASE_EXIST_OBSERVATION_NAME, storageType).observeChecked(callable);
    }

    private Observation createObservation(String name, StorageType storageType) {
        return Observation.createNotStarted(name, observationRegistry)
            .lowCardinalityKeyValue(STORAGE_TYPE_TAG_NAME, storageType.name());
    }

    private void recordCaseSize(long size, StorageType storageType) {
        DistributionSummary.builder(CASE_SIZE_METER_NAME)
            .tags(STORAGE_TYPE_TAG_NAME, storageType.name())
            .register(meterRegistry)
            .record(size);
    }
}
