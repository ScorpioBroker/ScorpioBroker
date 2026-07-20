---
type: NGSI-LD Clause
title: "Foreword"
description: "The following operations operate on an array of entities (as input payload): - Batch Entity Creation (clause 5.6.7)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11.0
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11.0"
---

The following operations operate on an array of entities (as input payload):

- Batch Entity Creation (clause 5.6.7).
- Batch Entity Creation or Update (Upsert) (clause 5.6.8).
- Batch Entity Update (clause 5.6.9).
- Batch Entity Delete (clause 5.6.10).
- Batch Entity Merge (clause 5.6.20). It is allowed for such an input Entity array to contain more than one instance of the same entity (those instances have identical ids). In order for such a request to be correctly handled, those instances that have the same id are processed by the Broker in the order they have in the array: the higher the index in the array, the later it will be processed. If the order is altered, the outcome may be altered. All Entities and Attributes in the batch will get the same modifiedAt timestamp, so it makes sense to distinguish them via the observedAt temporal property. Implementations shall treat the entity instances as if they had all arrived in separate requests. The following clauses specify the behaviour in each case.

# Related

* [Clause 5.6.10](/api-operations/provision/batch-entity-delete.md)
* [Clause 5.6.20](/api-operations/provision/batch-entity-merge.md)
* [Clause 5.6.7](/api-operations/provision/batch-entity-creation.md)
* [Clause 5.6.8](/api-operations/provision/batch-entity-creation-or-update-upsert.md)
* [Clause 5.6.9](/api-operations/provision/batch-entity-update.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11.0](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Foreword
