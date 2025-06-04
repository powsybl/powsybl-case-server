/**
 * Copyright (c) 2025, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.service;


import org.springframework.stereotype.Service;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.NonNull;
import io.micrometer.observation.Observation.CheckedCallable;

/**
 * @author AJELLAL Ali <ali.ajellal@rte-france.com>
 */
@Service
public class CaseObserver {
    protected static final String OBSERVATION_PREFIX = "app.case.";
    protected static final String FILE_SIZE_TAG_NAME = "file_size_bytes";
    protected static final String OPERATION_TYPE_TAG_NAME = "operation_type";

    private final ObservationRegistry observationRegistry;

    public CaseObserver(@NonNull ObservationRegistry observationRegistry) {
        this.observationRegistry = observationRegistry;
    }


    public <T, E extends Throwable> T observe(
            String name,
            Long fileSize,
            String operationType,
            CheckedCallable<T, E> callable
    ) throws E {
        return createObservation(name, fileSize, operationType).observeChecked(callable);
    }


    public <E extends Throwable> void observe(String name, String operationType, Observation.CheckedRunnable<E> runnable) throws E {
        createObservation(name, operationType).observeChecked(runnable);
    }

    private Observation createObservation(String name, Long fileSize, String operationType) {
        Observation observation = createObservation(name, operationType);
        if (fileSize != null) {
            observation.highCardinalityKeyValue(FILE_SIZE_TAG_NAME, String.valueOf(fileSize));
        }
        return observation;
    }


    private Observation createObservation(String name, String operationType) {
        return Observation.createNotStarted(OBSERVATION_PREFIX + name, observationRegistry)
                .lowCardinalityKeyValue(OPERATION_TYPE_TAG_NAME, operationType);
    }

}
