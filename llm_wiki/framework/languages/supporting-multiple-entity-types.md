---
type: NGSI-LD Clause
title: "Supporting Multiple Entity Types"
description: "From NGSI-LD API version 1.5.1 onwards, multiple Entity Types for any Entity are supported."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.16
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.16"
---

From NGSI-LD API version 1.5.1 onwards, multiple Entity Types for any Entity are supported. An Entity is uniquely identified by its id, so whenever information is provided for an Entity with a given id, it is considered part of the same Entity, regardless of the Entity Type(s) specified. To avoid unexpected behaviour, Entity Types can be implicitly added by all operations that update or append attributes. There is no operation to remove Entity Types from an Entity. The philosophy here is to assume that an Entity always had all Entity Types, but possibly not all Entity Types have previously been known in the system. The only option to remove an Entity Type is to delete the Entity and re-create it with the same id. Alternatively, a batch upsert with 'replace' update mode can be used, as described in clause 5.6.8.

# Related

* [Clause 5.6.8](/api-operations/provision/batch-entity-creation-or-update-upsert.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.16](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Supporting Multiple Entity Types
