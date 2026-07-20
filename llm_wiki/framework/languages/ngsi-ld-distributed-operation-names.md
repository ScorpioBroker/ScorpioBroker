---
type: NGSI-LD Clause
title: "NGSI-LD Distributed Operation names"
description: "When registering Context Sources (see clause 5.2.9), the registrant NGSI-LD interface endpoint may optionally offer a subset of NGSI-LD operations which it accepts."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.20
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.20"
---

When registering Context Sources (see clause 5.2.9), the registrant NGSI-LD interface endpoint may optionally offer a subset of NGSI-LD operations which it accepts. Table 4.20-1 defines a list of names for each of these operations.

Table 4.20-1: Names of implemented Operations Operation name Implements

Context Information Provision

createEntity 5.6.1 Create Entity updateEntity 5.6.2 Update Attributes appendAttrs 5.6.3 Append Attributes updateAttrs 5.6.4 Partial Attribute update deleteAttrs 5.6.5 Delete Attribute deleteEntity 5.6.6 Delete Entity createBatch 5.6.7 Batch Entity Creation upsertBatch 5.6.8 Batch Entity Creation or Update (Upsert) updateBatch 5.6.9 Batch Entity Update deleteBatch 5.6.10 Batch Entity Delete upsertTemporal 5.6.11 Create or Update Temporal Evolution of an Entity appendAttrsTemporal 5.6.12 Add Attributes to Temporal Evolution of an Entity deleteAttrsTemporal 5.6.13 Delete Attributes from Temporal Evolution of an Entity updateAttrInstanceTemporal 5.6.14 Partial Update Attribute Instance in Temporal Evolution of an Entity deleteAttrInstanceTemporal 5.6.15 Delete Attribute Instance from Temporal Evolution of an Entity deleteTemporal 5.6.16 Delete Temporal Evolution of an Entity mergeEntity 5.6.17 Merge Entity replaceEntity 5.6.18 Replace Entity replaceAttrs 5.6.19 Replace Attribute mergeBatch 5.6.20 Batch Entity Merge purgeEntity 5.6.21 Purge Entities

Context Information

retrieveEntity 5.7.1 Retrieve Entity queryEntity 5.7.2 Query Entities (excluding batch entity queries)

| Consumption | queryBatch | | | | | 5.7.2 Query Entities (batch entity queries only) | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | retrieveTemporal | | | | | 5.7.3 Retrieve Temporal Evolution of an Entity | | | | |
| | queryTemporal | | | | | 5.7.4 Query Temporal Evolution of Entities | | | | |
| | retrieveEntityTypes | | | | | 5.7.5 Retrieve Available Entity Types | | | | |
| | retrieveEntityTypeDetails | | | | | 5.7.6 Retrieve Details of Available Entity Types | | | | |
| | retrieveEntityTypeInfo | | | | | 5.7.7 Retrieve Available Entity Type Information | | | | |
| | retrieveAttrTypes | | | | | 5.7.8 Retrieve Available Attributes | | | | |
| | retrieveAttrTypeDetails | | | | | 5.7.9 Retrieve Details of Available Attributes | | | | |
| | retrieveAttrTypeInfo | | | | | 5.7.10 Retrieve Available Attribute Information | | | | |
| Context | createSubscription | | | | | 5.8.1 Create Subscription | | | | |
| Information | updateSubscription | | | | | 5.8.2 Update Subscription | | | | |
| Subscription | retrieveSubscription | | | | | 5.8.3 Retrieve Subscription | | | | |
| | querySubscription | | | | | 5.8.4 Query Subscription | | | | |
| | deleteSubscription | | | | | 5.8.5 Delete Subscription | | | | |
| Support | retrieveEntityMap | | | | | 5.14.1 Retrieve EntityMap | | | | |
| operations for | updateEntityMap | | | | | 5.14.2 Update EntityMap | | | | |
| distributed | deleteEntityMap | | | | | 5.14.3 Delete EntityMap | | | | |
| operations | createEntityMapQueryEntity | | | | | 5.14.4 Create EntityMap for Query Entities | | | | |

Operation name Implements createEntityMapQueryTemporal 5.14.5 Create EntityMap for Query Temporal Evolution of Entities retrieveContextSourceIdentity 5.15.1 Retrieve Context Source Identity Information

In addition to these individual operations, a series of names for common groups of operations have also been defined.
Table 4.20-2 defines a list of names for each of these operation groups.

Table 4.20-2: Named Operation Groups Operation Group name Implements federationOps - retrieveEntity

- queryEntity
- queryBatch
- retrieveEntityTypes
- retrieveEntityTypeDetails
- retrieveEntityTypeInfo
- retrieveAttrTypes
- retrieveAttrTypeDetails
- retrieveAttrTypeInfo
- createSubscription
- updateSubscription
- retrieveSubscription
- querySubscription
- deleteSubscription
- retrieveEntityMap
- updateEntityMap
- deleteEntityMap
- createEntityMapQueryEntity
- retrieveContextSourceIdentity associationOps - retrieveEntity
- queryEntity
- queryBatch
- retrieveEntityTypes
- retrieveEntityTypeDetails
- retrieveEntityTypeInfo
- retrieveAttrTypes
- retrieveAttrTypeDetails
- retrieveAttrTypeInfo
- createSubscription
- updateSubscription
- retrieveSubscription
- querySubscription
- deleteSubscription
- retrieveContextSourceIdentity


Operation Group name Implements updateOps - updateEntity

- updateAttrs
- replaceEntity
- replaceAttrs retrieveOps - retrieveEntity
- queryEntity redirectionOps - createEntity
- updateEntity
- appendAttrs
- updateAttrs
- deleteAttrs
- deleteEntity
- mergeEntity
- replaceEntity
- replaceAttrs
- retrieveEntity
- queryEntity
- purgeEntity
- retrieveEntityTypes
- retrieveEntityTypeDetails
- retrieveEntityTypeInfo
- retrieveAttrTypes
- retrieveAttrTypeDetails
- retrieveAttrTypeInfo
- retrieveEntityMap
- updateEntityMap
- deleteEntityMap
- createEntityMapQueryEntity
- retrieveContextSourceIdentity

If no specific subset of operations is defined for a Context Source Registration, the default set of operations matches the group defined as "federationOps".

# Related

* [Clause 5.2.9](/api-operations/data-types/csourceregistration.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.20](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD Distributed Operation names
