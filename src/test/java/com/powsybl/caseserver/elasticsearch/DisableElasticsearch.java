/*
 * Copyright (c) 2023, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.powsybl.caseserver.elasticsearch;

import org.elasticsearch.client.RestClient;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.test.context.TestPropertySource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@TestPropertySource(properties = {
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchRepositoriesAutoConfiguration"
})
public @interface DisableElasticsearch {
    class MockConfig {
        @Bean
        public EmbeddedElasticsearch embeddedElasticsearch() {
            return Mockito.mock(EmbeddedElasticsearch.class);
        }

        @Bean
        public ElasticsearchOperations elasticsearchOperations() {
            return Mockito.mock(ElasticsearchOperations.class);
        }

        @Bean
        public CaseInfosRepository caseInfosRepository() {
            return Mockito.mock(CaseInfosRepository.class);
        }

        @Bean
        public RestClient restClient() {
            return Mockito.mock(RestClient.class);
        }

    }
}
