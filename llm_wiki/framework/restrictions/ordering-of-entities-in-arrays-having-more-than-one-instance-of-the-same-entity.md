---
type: NGSI-LD Clause
title: "Ordering of Entities in arrays having more than one instance of the same Entity"
description: "Some services (batch operations, clauses 5.6.7, 5.6.8, 5.6.9 and 5.6.10) operate on an array of entities, as input, and if this array contains more than one instance of the same entity, then these ent"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.6.6
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.6.6"
---

Some services (batch operations, clauses 5.6.7, 5.6.8, 5.6.9 and 5.6.10) operate on an array of entities, as input, and if this array contains more than one instance of the same entity, then these entity instances shall come in chronological order, i.e. the first entity instance in the array shall be older than the second, the second shall be older than the third, etc. Without this assumption, there is no way for the request to be treated correctly, as the entity instances are often used for replacing or modifying the prior entity instance.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.6.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Ordering of Entities in arrays having more than one instance of the same Entity
