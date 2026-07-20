---
type: NGSI-LD Clause
title: "Batch Entity Merge case"
description: "The Batch Entity Merge operation has as input an array of Entity IDs, for the entities to be merged."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11.5"
---

The Batch Entity Merge operation has as input an array of Entity IDs, for the entities to be merged. If an Entity ID is replicated in the array, these merge operations shall be done in chronological order. Only the entity resulting from merging all of the entity instances, in the correct order, is maintained in the current state (as defined in clause 4.3.1). For Temporal Evolution of Entities (as defined in clause 4.3.1), all entity instances shall be taken into account, in the correct order.

# Related

* [Clause 4.3.1](/framework/architecture/introduction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Merge case
