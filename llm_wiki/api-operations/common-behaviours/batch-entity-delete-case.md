---
type: NGSI-LD Clause
title: "Batch Entity Delete case"
description: "The Batch Entity Delete operation has as input an array of Entity IDs, for the entities to be deleted."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11.4"
---

The Batch Entity Delete operation has as input an array of Entity IDs, for the entities to be deleted. If an Entity ID is replicated in the array, the first occurrence will delete the entity, while subsequent occurrences of the same Entity ID will provoke an error in the response (entity does not exist).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Batch Entity Delete case
