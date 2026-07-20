---
type: NGSI-LD Operation
title: "Retrieve Temporal Evolution of an Entity"
description: "5.7.3.1 Description This operation allows retrieving the Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.3"
---

5.7.3.1 Description

This operation allows retrieving the Temporal Evolution of an Entity.

5.7.3.2 Use case diagram

A Context Consumer can retrieve the Temporal Evolution of an Entity (in the form of a temporal representation) from an NGSI-LD system as shown in Figure 5.7.3.2-1.

Figure 5.7.3.2-1: Retrieve Temporal Evolution of an Entity use case

5.7.3.3 Input data

- Entity ID (URI) of the target Temporal Evolution of an Entity to be retrieved.
- List of Attribute (Properties or Relationships) names to be retrieved (projection attributes) (optional).

Context Consumer

Retrieve Temporal evolution of an Entity

NGSI-LD Client

NGSI-LD System

Response with (Entity Temporal)

1..*

EntityTemporal retrieve temporal evolution of an entity (entityId)


- A restrictive list of Entity member names ("id", "type", "scope" or an Attribute name) to be retrieved (projection attributes as defined by clause 4.21) (optional).
- An exclusionary list of Entity member names ("id", "type", "scope" or an Attribute name) to be removed (projection attributes as defined by clause 4.21) (optional).
- An NGSI-LD temporal query as mandated by clause 4.11 (optional).
- A parameter (lastN) conveying that only the last N instances (per Attribute) within the concerned temporal interval shall be retrieved (optional).
- An optional JSON-LD context.
- A flag indicating whether to return the location of the EntityMap used within the operation (optional).
- A suggested lifetime for the EntityMap, if EntityMap is to be created (optional).
- The location of a resource holding an EntityMap of matching Entity registrations (optional).
- A datasetId parameter that specifies which Attribute instances are to be selected as defined by clause 4.5.5 (optional).

5.7.3.4 Behaviour

- If the Entity ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If projection attributes are present and indicate the use of Linked Entity retrieval, an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity whose id (URI) is equivalent held locally, and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- The lastN parameter refers to a number, n, of Attribute instances which shall correspond to the last n timestamps (in descending ordering) of the temporal property (by default observedAt) within the concerned temporal interval.
- Let S be the Temporal Evolution of the Entity as mandated by clause 5.2.20 with the specified Entity ID as it is available locally. S is empty in case no Temporal Evolution of the Entity is available locally.
- From S, select only those Attribute instances (corresponding to the Attributes specified by the query or all if none are specified) match the temporal restrictions imposed by the temporal query (as mandated by clause 4.11); i.e. if the time series, for all the concerned Attributes of an Entity, does not include data corresponding to the temporal query interval, then such Entity shall be removed from S, thus it shall not appear in the final result set. Let S1 be this new subset.
- If the ContextBroker implementation supports the use of EntityMaps then: - If the location of a resource holding an EntityMap of matching Entity registrations is present it shall be retrieved: - If the resource cannot be found, or the data has expired, a new EntityMap shall be created. - If the data has not expired, only the retrieved Entity Map shall be used to determine which Context Source Registrations match the Entity ID. - If a flag to return an EntityMap was present in the request, and no EntityMap currently exists, then a new EntityMap shall be created.


- For Context Source Registrations that match the Entity ID and support the "retrieveTemporal" operation (see operations and operation groups in clause 4.20), implementations shall do the following: - If an EntityMap is in use for this operation, and an EntityMap entry linked to a Context Source Registration is found, the location of the linked EntityMap shall be passed as part of any forwarded request. - For any exclusive, redirect and inclusive Context Source, the request is forwarded for remote retrieval by matching endpoints. and remote Attribute data for the Entity is received. The result is then merged together with S1 according to the algorithm defined in clause 4.5.5. - For any auxiliary Context Source Registrations the remote Attribute data received is added to S1 only when the Attribute instance, whose value of the timeproperty, which is used for the temporal query (observedAt as default), is not present in any of the Attribute instances received from elsewhere. - If an EntityMap is in use for this operation, the EntityMap's linked maps are updated to hold the location of every EntityMap used by the Context Source Registrations.
- For the set of Attribute Instances that are in S1, when datasetId parameter is provided in the request: - Filter the Attribute instances based on the datasetId parameter, i.e. keep only the Attribute instances whose datasetId is specified. The default Attribute instance is matched, if "@none" is specified. - If there is no Attribute instance whose datasetId matches the value of the parameter, remove the complete Attribute from S1.
- From the set of Attribute Instances that are in S1, include in their temporal representation only the Attribute instances (up to lastN) corresponding to the query's projection Attributes, or aggregated values of Attribute instances (if aggregated temporal representation is requested). In the general case, it is not possible to retrieve a set of entities by only specifying desired Entity identifiers, without further specifying restrictions on the entities' types or attributes, either explicitly, via selector of Entity types or of Attribute names, or implicitly, within an NGSI-LD Query or GeoQuery. If the execution of the operation is limited to the local scope (see clause 5.5.13), no further restrictions have to be provided.

5.7.3.5 Output data

A JSON-LD object representing the Temporal Evolution of the target Entity as mandated by clause
5.2.20.
If a restrictive list of Entity member names is present, every Entity within the payload body is reduced down to only
contain the defined Entity members.
If an exclusionary list of Entity member names is present, the defined Entity members listed are removed from each
Entity within the payload.

# Related

* [HTTP: Resource: temporal/entities/{entityId}](/http-binding/resource-temporal-entities-entityid.md)
* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 5.2.20](/api-operations/data-types/entitytemporal.md)
* [Clause 5.5.13](/api-operations/common-behaviours/limiting-operations-to-local-scope.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Temporal Evolution of an Entity
