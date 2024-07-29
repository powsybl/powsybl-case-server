/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.elasticsearch;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchCustomConversions;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;

/**
 * A class to configure DB elasticsearch client for metadatas transfer
 *
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */

@Configuration
@EnableElasticsearchRepositories
public class ESConfig extends ElasticsearchConfiguration {

    public static final String CASE_INFOS_INDEX_NAME = "#{@environment.getProperty('powsybl-ws.elasticsearch.index.prefix')}cases";//FIXME (jamal) check this name


    @Value("#{'${spring.data.elasticsearch.embedded:false}' ? 'localhost' : '${spring.data.elasticsearch.host}'}")
    private String esHost;

    @Value("#{'${spring.data.elasticsearch.embedded:false}' ? '${spring.data.elasticsearch.embedded.port:}' : '${spring.data.elasticsearch.port}'}")
    private int esPort;

    @Value("${spring.data.elasticsearch.client.timeout:60}")
    private int timeout;

    @Value("${spring.data.elasticsearch.username:#{null}}")
    private Optional<String> username;

    @Value("${spring.data.elasticsearch.password:#{null}}")
    private Optional<String> password;

    //It should be detected without specifying the name, but it isn't. To investigate.
    @Bean(name = "elasticsearchClientConfiguration")
    @Override
    public ClientConfiguration clientConfiguration() {
        var clientConfiguration = ClientConfiguration.builder()
                .connectedTo(esHost + ":" + esPort)
                .withConnectTimeout(timeout * 1000L).withSocketTimeout(timeout * 1000L);

        if (username.isPresent() && password.isPresent()) {
            clientConfiguration.withBasicAuth(username.get(), password.get());
        }

        return clientConfiguration.build();
    }

    @Override
    public ElasticsearchCustomConversions elasticsearchCustomConversions() {
        return new ElasticsearchCustomConversions(Arrays.asList(DateToStringConverter.INSTANCE, StringToDateConverter.INSTANCE));
    }

    @WritingConverter
    enum DateToStringConverter implements Converter<ZonedDateTime, String> {
        INSTANCE;

        @Override
        public String convert(ZonedDateTime date) {
            return date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
    }

    @ReadingConverter
    enum StringToDateConverter implements Converter<String, ZonedDateTime> {
        INSTANCE;

        @Override
        public ZonedDateTime convert(String s) {
            return ZonedDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
    }
}
