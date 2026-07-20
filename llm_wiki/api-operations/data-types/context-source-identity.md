---
type: NGSI-LD Data Type
title: "Context Source Identity"
description: "This type represents the data uniquely identifying a Context Source, and if the Context Source supports multi-tenancy (see clause 4.14) uniquely identifying a Tenant within that Context Source."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.2.40
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.2.40"
---

This type represents the data uniquely identifying a Context Source, and if the Context Source supports multi-tenancy (see clause 4.14) uniquely identifying a Tenant within that Context Source.

The supported JSON members shall follow the requirements provided in Table 5.2.40-1.

| id | String | Valid URI | | | 1 | Context Source ID. | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| type | String | It shall be equal to | | | 1 | JSON-LD @type. | | | | |
| | | "ContextSourceIden | | | | | | | | |
| | | tity" | | | | | | | | |
| contextSourceAlias | String | Non-empty string. | | | 1 | A unique id for a Context | | | | |
| | | Pseudonym field as defined | | | | Source which can be used | | | | |
| | | in IETF RFC 7230 [27] | | | | to identify loops. | | | | |
| | | | | | | In the multi-tenancy use | | | | |
| | | | | | | case (see clause 4.14), this | | | | |
| | | | | | | id shall be identifying a | | | | |

Table 5.2.40-1: Context Source Identity data type definition Name Data Type Restriction Cardinality Description specific Tenant within a registered Context Source.

| contextSourceUptime | String | String representing a | | | 1 | Total Duration that the | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | duration in ISO 8601 [17] | | | | Context Source has | | | | |
| | | format | | | | been available. | | | | |
| contextSourceTimeAt | String | DateTime (clause 4.6.3) | | | 1 | Current time observed at the | | | | |
| | | | | | | Context Source. | | | | |
| | | | | | | Timestamp See clause 4.8. | | | | |
| contextSourceExtras | JSON | | | | 0..1 | Instance specific information | | | | |
| | | | | | | relevant to the configuration | | | | |
| | | | | | | of the Context Source | | | | |
| | | | | | | itself in raw un-expandable | | | | |

JSON which shall not be interpreted as JSON-LD using the supplied @context.

# Related

* [Clause 4.14](/framework/languages/supporting-multiple-tenants.md)
* [Clause 4.6.3](/framework/restrictions/supported-data-types-for-values.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.2.40](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Source Identity
