---
type: NGSI-LD Clause
title: "More than one instance of the same Entity in an Entity array"
description: "5.5.11.0 Foreword The following operations operate on an array of entities (as input payload): - Batch Entity Creation (clause 5.6.7)."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.5.11
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.5.11"
---

5.5.11.0 Foreword

The following operations operate on an array of entities (as input payload):

- Batch Entity Creation (clause 5.6.7).
- Batch Entity Creation or Update (Upsert) (clause 5.6.8).
- Batch Entity Update (clause 5.6.9).
- Batch Entity Delete (clause 5.6.10).
- Batch Entity Merge (clause 5.6.20). It is allowed for such an input Entity array to contain more than one instance of the same entity (those instances have identical ids). In order for such a request to be correctly handled, those instances that have the same id are processed by the Broker in the order they have in the array: the higher the index in the array, the later it will be processed. If the order is altered, the outcome may be altered. All Entities and Attributes in the batch will get the same modifiedAt timestamp, so it makes sense to distinguish them via the observedAt temporal property. Implementations shall treat the entity instances as if they had all arrived in separate requests. The following clauses specify the behaviour in each case.

5.5.11.1 Batch Entity Creation case

The first occurrence of an entity in the input array (the oldest one) is used for the creation of the entity. Any subsequent instance of the same entity is reported as an error (entity already exists) in the response.

5.5.11.2 Batch Entity Creation or Update (Upsert) case

This operation has two modes of operation, with an optional flag to select between the two. The default behaviour is to
replace any already existing entities, while the optional behaviour is to update already existing entities. Non existing
entities are created in both modes.
If the entity does not yet exist, the first occurrence of an entity is used to create the entity, and subsequent instances of
that same entity are used to either replace (default behaviour) or to update (optional behaviour) the entity. These replace
or update operations shall be done in chronological order.
Only the entity resulting from merging all of the entity instances, in the correct order, is maintained in the current state
(as defined in clause 4.3.1). For Temporal Evolution of Entities (as defined in clause 4.3.1), all entity
instances shall be taken into account, in the correct order.

5.5.11.3 Batch Entity Update case

This operation has two modes of operation, with an optional flag to select between the two. The default behaviour is to replace any already existing attributes of the entities, while the optional behaviour is to preserve already existing attributes of the entities. Brokers shall send separate notifications for each individual update, taking throttling into account.

5.5.11.4 Batch Entity Delete case

The Batch Entity Delete operation has as input an array of Entity IDs, for the entities to be deleted. If an Entity ID is replicated in the array, the first occurrence will delete the entity, while subsequent occurrences of the same Entity ID will provoke an error in the response (entity does not exist).


5.5.11.5 Batch Entity Merge case

The Batch Entity Merge operation has as input an array of Entity IDs, for the entities to be merged. If an Entity ID is replicated in the array, these merge operations shall be done in chronological order. Only the entity resulting from merging all of the entity instances, in the correct order, is maintained in the current state (as defined in clause 4.3.1). For Temporal Evolution of Entities (as defined in clause 4.3.1), all entity instances shall be taken into account, in the correct order.

# Related

* [Clause 4.3.1](/framework/architecture/introduction.md)
* [Clause 5.6.10](/api-operations/provision/batch-entity-delete.md)
* [Clause 5.6.20](/api-operations/provision/batch-entity-merge.md)
* [Clause 5.6.7](/api-operations/provision/batch-entity-creation.md)
* [Clause 5.6.8](/api-operations/provision/batch-entity-creation-or-update-upsert.md)
* [Clause 5.6.9](/api-operations/provision/batch-entity-update.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.5.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — More than one instance of the same Entity in an Entity array
