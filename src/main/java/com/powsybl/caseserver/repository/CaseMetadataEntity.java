/**
 * Copyright (c) 2022, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.powsybl.caseserver.repository;

import java.time.Instant;
import java.util.UUID;

import lombok.*;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * @author Abdelsalem Hedhili <abdelsalem.hedhili at rte-france.com>
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Entity
@Table(name = "caseMetadata")
public class CaseMetadataEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "expirationDate", columnDefinition = "timestamptz")
    private Instant expirationDate;

    @Column(name = "indexed", columnDefinition = "boolean default false", nullable = false)
    private boolean indexed = false;

    @Column(name = "originalFilename", columnDefinition = "Original case file name")
    private String originalFilename;

    @Column(name = "compressionFormat", columnDefinition = "Case compression format")
    private String compressionFormat;

    @Column(name = "format", columnDefinition = "Case format")
    private String format;
}
