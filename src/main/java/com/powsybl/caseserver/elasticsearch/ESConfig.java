/**
 * Copyright (c) 2020, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.elasticsearch;

import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.elasticsearch.config.ElasticsearchConfigurationSupport;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchCustomConversions;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * A class to configure DB elasticsearch client for metadatas transfer
 *
 * @author Slimane Amar <slimane.amar at rte-france.com>
 */

@Configuration
public class ESConfig extends ElasticsearchConfigurationSupport {

    public static final String CASE_INFOS_INDEX_NAME = "#{@environment.getProperty('powsybl-ws.elasticsearch.index.prefix')}cases";

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
