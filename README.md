# PowSyBl Case Server

[![Actions Status](https://github.com/powsybl/powsybl-case-server/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/powsybl/powsybl-case-server/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=com.powsybl%3Apowsybl-case-server&metric=coverage)](https://sonarcloud.io/component_measures?id=com.powsybl%3Apowsybl-case-server&metric=coverage)
[![MPL-2.0 License](https://img.shields.io/badge/license-MPL_2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)
[![Slack](https://img.shields.io/badge/slack-powsybl-blueviolet.svg?logo=slack)](https://join.slack.com/t/powsybl/shared_invite/zt-36jvd725u-cnquPgZb6kpjH8SKh~FWHQ)

## Description

The **powsybl-case-server** is a microservice of the [GridSuite](https://github.com/gridsuite) platform dedicated to **storing and serving power network case files** in any format supported by [PowSyBl](https://www.powsybl.org) (CGMES, UCTE, XIIDM, Matpower, IEEE-CDF, …).

It provides the following capabilities:

- **Import cases**: upload a case file (plain, GZ-compressed, ZIP archive, or TAR archive). Archives are extracted and each sub-file is individually stored compressed in S3 to allow efficient sub-file access by network-conversion-server.
- **Download cases**: stream the original case file back to the caller.
- **Duplicate cases**: server-side S3-to-S3 copy without downloading the file to the service.
- **Expose a datasource API**: allow network-conversion-server to query individual sub-files within an archive case (existence check, list by regex, byte-range reads) without re-downloading the whole archive.
- **Index cases in Elasticsearch**: when indexation is requested, case metadata (name, format, ENTSOE/CGMES-specific attributes) are indexed to support Lucene full-text search queries.
- **Manage case expiration**: cases can be created with a TTL (1 hour); a scheduled job (backed by ShedLock) periodically deletes expired cases from S3, PostgreSQL, and Elasticsearch.
- **Notify** other microservices via RabbitMQ whenever a case is imported or duplicated.

---

## Technical Stack

- Spring Boot (Web, Data JPA, Actuator)
- PostgreSQL + Liquibase (case metadata)
- Amazon S3 (AWS SDK v2) (case file storage)
- Elasticsearch ( case indexation)
- RabbitMQ via Spring Cloud Stream
- ShedLock (distributed scheduler lock for expiration cleanup)
- PowSyBl importers: CGMES, UCTE, XIIDM, Matpower, IEEE-CDF
- API documentation: OpenAPI / Swagger (`springdoc`)
- Micrometer / Prometheus

---

## Development Scripts

Build Docker image:

```shell
mvn install -DskipTests -Dpowsybl.docker.install
```

Please read [liquibase usage](https://github.com/powsybl/powsybl-parent/#liquibase-usage) for instructions to automatically generate changesets. After you generated a changeset do not forget to add it to git and in `src/main/resources/db/changelog/db.changelog-master.yml`.

---

## Storage Model

Cases are stored in S3 under a configurable root path with the layout `<rootDirectory>/<caseUuid>/<filename>`.

| Case type | Storage |
|---|---|
| Plain file (e.g. `.xml`) | Compressed to GZIP and stored as `<filename>.gz` |
| Already-compressed file (e.g. `.xml.gz`) | Stored as-is |
| ZIP / TAR archive | Original archive stored as-is; each sub-file individually extracted and stored as `<subfile>.gz` |

Case metadata (original filename, format, compression format, expiration date, indexation flag) are persisted in PostgreSQL.

---

## Interactions with Other Microservices

```text
┌──────────────────────────┐
│   powsybl-case-server    │
└──────────────────────────┘
          ▼
       RabbitMQ (publishCaseImport — emitted on every case import or duplication)
```

The case-server does not call other microservices. It is consumed by **network-conversion-server**, which reads case files via the datasource API to convert them into the network store.

---

## Micrometer Observability

Key operations are wrapped in named Micrometer observations via `CaseObserver`:

---

## Useful Links

- [PowSyBl supported formats](https://www.powsybl.org/pages/documentation/index.html#grid-formats)
