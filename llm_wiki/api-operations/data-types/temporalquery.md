---
type: NGSI-LD Data Type
title: "TemporalQuery"
description: "This datatype represents a temporal query."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.21
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.21"
---

This datatype represents a temporal query.
The supported JSON members shall follow the requirements provided in Table 5.2.21-1.

Table 5.2.21-1: TemporalQuery data type definition Name Data Type Restrictions Cardinality Description timeAt String representing the timeAt parameter as defined by clause 4.11

It shall be a DateTime 1

timerel String Allowed values: "before", "after" and "between"

1 Represents the temporal relationship as defined by clause 4.11.

| aggrMethods | Comma separated list of string | It shall be 1 if | | 0..1 | Each String represents | |
| --- | --- | --- | --- | --- | --- | --- |
| | | aggregatedValues is present | | | an aggregation | |
| | | in the options parameter | | | method, as defined by | |

0..1 Each String represents an aggregation method, as defined by clause 4.5.19. Only applicable if "aggregatedValu es" is present in the format or options parameter.

aggrPeriodDu ration

String 0..1 It represents the duration of each period used for the aggregation as defined by clause 4.5.19. If not specified, it defaults to a duration of 0 seconds and is interpreted as a duration spanning the whole time-range specified by the temporal query. Only applicable if "aggregatedValu es" is present in the format or options parameter.

0..1

endTimeAt String representing the endTimeAt parameter as defined by clause 4.11

It shall be a DateTime. Cardinality shall be 1 if timerel is equal to "between"

lastN Positive integer 0..1 Only the last n instances, per Attribute, per Entity (under the specified time interval) shall be retrieved.

0..1

timeproperty String representing a Temporal Property name

Allowed values: "observedAt", "createdAt", "modifiedAt" and "deletedAt". If not specified, the default is "observedAt". (See clause 4.8)

# Related

* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.5.19](/framework/data-representation/aggregated-temporal-representation-of-an-entity.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.21](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — TemporalQuery
