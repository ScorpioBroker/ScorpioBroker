---
type: NGSI-LD Data Type
title: "TimeInterval"
description: "The supported JSON members shall follow the requirements provided in Table 5.2.11-1."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.11
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.11"
---

The supported JSON members shall follow the requirements provided in Table 5.2.11-1.

| Name Data Type | | Restrictions | | Cardinality | | | Description | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| startAt String | | DateTime (clause 4.6.3) | | 1 | Describes the start of the time interval | | | | |
| endAt String | | DateTime (clause 4.6.3) | | 0..1 | Describes the end of the time interval. If not present | | | | |
| | | | | | the interval is open | | | | |

Table 5.2.11-1: TimeInterval data type definition

# Related

* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — TimeInterval
