---
type: NGSI-LD Operation
title: "Retrieve Available Entity Types"
description: "5.7.6; 6.25.3.1 Entity Type /types/{type} GET Details about available entity type 5.7.7; 6.26.3.1 Attributes /attributes/ GET Available attributes 5.7.8 and 5.7.9; 6.27.3.1 Attribute /attributes/{attr"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.5"
---

5.7.6; 6.25.3.1

Entity Type /types/{type} GET Details about available entity
type 5.7.7; 6.26.3.1
Attributes /attributes/ GET Available attributes 5.7.8 and
5.7.9; 6.27.3.1

Attribute /attributes/{attrId} GET Details about available attribute

5.7.10;
6.28.3.1

Context source registration list /csourceRegistrations/

POST Csource registration creation 5.9.2; 6.8.3.1
GET Discover Csource
registrations

5.10.2; 6.8.3.2

GET Csource registration retrieval by id 5.10.1; 6.9.3.1 PATCH Csource registration update by id 5.9.3; 6.9.3.2

Context source registration by Id /csourceRegistrations/{registrationId}

Csource registration deletion by id 5.9.4; 6.9.3.3

DELETE

| | | | POST | Create subscription to | 5.11.2; |
| --- | --- | --- | --- | --- | --- |
| Context source | | | | Csource registration | 6.12.3.1 |
| registration | | /csourceSubscriptions/ | | Retrieval of list of | |
| subscription list | | | GET | subscription to Csource | 5.11.5; |
| | | | | registration | 6.12.3.2 |

GET Csource registration subscription retrieval by id

5.11.4;
6.13.3.1

Context source registration subscription by Id

/csourceSubscriptions/{subscriptionId}

PATCH Csource registration subscription update by id

5.11.3;
6.13.3.2

Csource registration subscription deletion by id

5.11.6;
6.13.3.3

DELETE

Entity Operations.
Create /entityOperations/create POST Batch Entity creation 5.6.7; 6.14.3.1
Entity Operations.
Upsert /entityOperations/upsert POST Batch Entity creation or
update (upsert) 5.6.8; 6.15.3.1
Entity Operations.
Update /entityOperations/update POST Batch Entity update 5.6.9; 6.16.3.1
Entity Operations.
Delete

/entityOperations/delete POST Batch Entity deletion 5.6.10; 6.17.3.1

Entity Operations. Query /entityOperations/query POST Query entities based on POST 5.7.2; 6.23.3.1

Entity Operations. Merge /entityOperations/merge POST Batch Entity merge 5.6.20; 6.31.3.1

| Temporal | | | | Entity creation | 6.18.3.1 |
| --- | --- | --- | --- | --- | --- |
| Evolution of | | /temporal/entities/ | | | |
| Entities | | | GET | Query Temporal Evolution of Entities | 5.7.4; 6.18.3.2 |

POST Temporal Evolution of an 5.6.11;


Resource Name Resource URI HTTP Method Meaning Clauses

GET

Temporal Evolution of an Entity retrieval by id 5.7.3; 6.19.3.1 DELETE Temporal Evolution of an Entity deletion by id

Temporal
Evolution of an
Entity by id

/temporal/entities/{entityId}

5.6.16;
6.18.3.2
5.6.12;
6.20.3.1

Temporal
Representation of
Attribute List

/temporal/entities/{entityId}/attrs/ POST

Temporal Evolution of Attribute of an Entity instance addition

Temporal
Representation of
Attribute by id

/temporal/entities/{entityId}/attrs/{attrId} DELETE

Attribute from Temporal Evolution of an Entity deletion

5.6.13;
6.21.3.1

| | | | | Attribute Instance from | 5.6.14; |
| --- | --- | --- | --- | --- | --- |
| Temporal | | | PATCH | Temporal Evolution of an | 6.22.3.1 |
| Representation of | | /temporal/entities/{entityId}/attrs/{attrId} | | Entity update by instance id | |
| Attribute Instance | | /{instanceId} | | Attribute Instance from | |
| by id | | | DELETE | Temporal Evolution of an | 5.6.15; |
| | | | | Entity deletion by instance id | 6.22.3.2 |

GET

Query temporal evolution of entities for creating entity map

5.15.4;
6.35.3.1

Create EntityMap
for Query
Temporal
Evolution of
Entities

/temporal/entityMaps/

POST

Query temporal evolution of entities for creating entity map based on POST

5.15.4;
6.35.3.2

| Temporal Query Operation | | /temporal/entityOperations/query | | POST | | Query Temporal Evolution of Entities based on POST | | 5.7.4; 6.24.3.1 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | POST | | Add a user @context to the | | 5.13.2; |
| Add and List @context | | /jsonldContexts | | | | internal cache | | 6.29.3.1 |
| | | | | GET | | List all cached @contexts | | 5.13.3; |
| | | | | | | | | 6.29.3.2 |
| | | | | GET | | Serve one specific user @context | | 5.13.4; 6.30.3.1 |
| Serve, Delete and | | | | | | Delete one specific | | |
| Reload @context | | /jsonldContexts/{contextId} | | | | @context from internal | | |
| | | | | DELETE | | cache, possibly re-inserting | | 5.13.5; |
| | | | | | | a freshly downloaded copy | | 6.30.3.2 |
| | | | | | | of it | | |
| | | | | GET | | Query entities for creating | | 5.14.4; |
| Create EntityMap for Query Entities | | /entityMaps/ | | | | entity map | | 6.34.3.1 |
| | | | | POST | | Query entities for creating | | 5.14.4; |
| | | | | | | entity map based on POST | | 6.34.3.2 |
| | | | | GET | | EntityMap Retrieval by id | | 5.14.1; 6.32.3.1 |
| Retrieve, Update | | | | | | | | |
| and Delete Entity | | /entityMaps/{entityMapId} | | PATCH | | EntityMap Update by id | | 5.14.2; |
| Maps | | | | | | | | 6.32.3.2 |
| | | | | DELETE | | EntityMap Deletion by id | | 5.14.3; 6.32.3.3 |

Retrieve Context Source Identity Information

/info/sourceIdentity GET Context Source Identity Retrieval

5.15.1;
6.33.3.1

| Create Snapshot | | | | | 6.36.3.1 |
| --- | --- | --- | --- | --- | --- |
| or Purge | | /snapshots/ | | | |
| Snapshots | | | DELETE | Purge Snapshots | 5.16.7; 6.36.3.2 |

POST Create Snapshot 5.16.1;

Retrieve and
Update Snapshot
Status or Delete
Snapshot
GET Retrieve Snapshot Status 5.16.3;
6.37.3.1
PATCH Update Snapshot Status 5.16.4;
6.37.3.2
DELETE Delete Snapshot 5.16.5;
6.37.3.3

/snapshots/{snapshotId}

5.16.2;
6.38.3.1

Clone Snapshot /snapshots/{snapshotId}/clone POST Clone Snapshot

# Related

* [HTTP: Resource: types/](/http-binding/resource-types.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Available Entity Types
