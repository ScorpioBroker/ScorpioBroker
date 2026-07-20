---
type: NGSI-LD Data Type
title: "AggregationParams"
description: "This datatype represents the parameters required for supporting aggregation methods in temporal requests."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.44
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.44"
---

This datatype represents the parameters required for supporting aggregation methods in temporal requests.
The supported JSON members shall follow the requirements provided in Table 5.2.44-1.


Table 5.2.44-1: AggregationParams data type definition Name Data Type Restriction Cardinality Description aggrMethods Comma separated list of strings

| aggrMethods | | Comma separated | Each String represents an | | 1 | - | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | list of strings | aggregation method, as | | | | | |
| | | | defined by clause 4.5.19. | | | | | |
| aggrPeriodDuration | | String | It represents the duration of | | 0..1 | - | | |
| | | | each period used for the | | | | | |

aggrPeriodDuration String It represents the duration of each period used for the aggregation as defined by clause 4.5.19. If not specified, it defaults to a duration of 0 seconds and is interpreted as a duration spanning the whole time-range specified by the temporal query.

# Related

* [Clause 4.5.19](/framework/data-representation/aggregated-temporal-representation-of-an-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.44](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — AggregationParams
