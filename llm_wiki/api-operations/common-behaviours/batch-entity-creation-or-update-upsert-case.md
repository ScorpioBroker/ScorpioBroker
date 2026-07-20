---
type: NGSI-LD Clause
title: "Batch Entity Creation or Update (Upsert) case"
description: "This operation has two modes of operation, with an optional flag to select between the two."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11.2"
---

This operation has two modes of operation, with an optional flag to select between the two. The default behaviour is to
replace any already existing entities, while the optional behaviour is to update already existing entities. Non existing
entities are created in both modes.
If the entity does not yet exist, the first occurrence of an entity is used to create the entity, and subsequent instances of
that same entity are used to either replace (default behaviour) or to update (optional behaviour) the entity. These replace
or update operations shall be done in chronological order.
Only the entity resulting from merging all of the entity instances, in the correct order, is maintained in the current state
(as defined in clause 4.3.1). For Temporal Evolution of Entities (as defined in clause 4.3.1), all entity
instances shall be taken into account, in the correct order.

# Related

* [Clause 4.3.1](/framework/architecture/introduction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Creation or Update (Upsert) case
