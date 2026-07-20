---
type: NGSI-LD Operation
title: "Query Temporal Evolution of Entities"
description: "5.7.4.1 Description This operation allows querying the Temporal Evolution of Entities present in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.4"
---

5.7.4.1 Description

This operation allows querying the Temporal Evolution of Entities present in an NGSI-LD system. It is similar to the operation defined by clause 5.7.2 (Query Entities) with the addition of a temporal query.

5.7.4.2 Use case diagram

A Context Consumer can retrieve the Temporal Evolution of a set of Entities which matches a specific query from an NGSI-LD system as shown in Figure 5.7.4.2-1.


Figure 5.7.4.2-1: Query Temporal Evolution of Entities use case

5.7.4.3 Input data

- An NGSI-LD temporal query as mandated by clause 4.11.
- A reference to a JSON-LD @context (optional).
- A selector of Entity types as specified by clause 4.17 (optional). Both type name (short hand string) and fully qualified type name (URI) are allowed.
- A restrictive list of Entity member names ("id", "type", "scope" or an Attribute name) to be retrieved (projection attributes as defined by clause 4.21) (optional).
- An exclusionary list of Entity member names ("id", "type", "scope" or an Attribute name) to be removed (projection attributes as defined by clause 4.21) (optional).
- A list (one or more) of Entity identifiers (optional).
- A list (one or more) of Attribute names of which at least one shall exist in order for an Entity to be selected, and also used as query projection attributes (optional).
- An id pattern as a regular expression (optional).
- An NGSI-LD query (to filter out Entities by Attribute values) as per clause 4.9 (optional).
- An NGSI-LD geoquery (to filter out Entities by spatial relationships) as mandated by clause 4.10 (optional).
- A NGSI-LD Scope query (to filter out Entities based on their Scope) as mandated by clause 4.19 (optional).
- An NGSI-LD query (called context source filter, to filter out Context Sources by the values of properties that describe them) as per clause 4.9 (optional).
- A limit to the number of Entities to be retrieved. See clause 5.5.9.
- A parameter (lastN) conveying that only the last N instances (per Attribute) within the concerned temporal interval shall be retrieved (optional).
- A specified language filter as per clause 4.15 (optional).
- A list (one or more) of Attribute names whose values shall be expanded to URIs prior to executing a query (optional).

Context Consumer

Query Temporal Evolution of Entities

NGSI-LD Client

NGSI-LD System

Response with (EntityTemporal List)

1..*

EntityTemporal Array query temporal evolution of entities (query)


- A flag indicating whether to return the location of the EntityMap used within the operation (optional).
- A suggested lifetime for the EntityMap, if EntityMap is to be created (optional).
- The location of a resource holding an EntityMap of matching Entity registrations (optional).
- A datasetId parameter that specifies which Attribute instances are to be selected as defined by clause 4.5.5 (optional).
- A preferred ordering of Entities retrieved - only the Entity member name "id" can be used (Entity ordering attributes as defined by clause 4.23) (optional).
- A preferred collation setting to be used when applying ordering of Entities (optional).
- A flag indicating whether split Entities are to be expected, i.e. Entities whose information is distributed across different Context Sources (optional). In the general case, it is not possible to retrieve a set of entities by only specifying desired Entity identifiers, without further specifying restrictions on the entities' types or attributes, either explicitly, via selector of Entity types or of Attribute names, or implicitly, within an NGSI-LD query or geoquery. If the execution of the operation is limited to the local scope (see clause 5.5.13), no further restrictions have to be provided.

5.7.4.4 Behaviour

- If a temporal query is not provided then an error of type BadRequestData shall be raised.
- At least one of the following input data shall be provided: a) selector of Entity Types; b) list of Attribute names, including at least one non-system Attribute; c) NGSI-LD Query, including at least one non-system Attribute; d) NGSI-LD GeoQuery; e) local scope (see clause 5.5.13). If none of the above is provided, then an error of type BadRequestData shall be raised (too wide query).
- If projection attributes or filter conditions indicate the use of Linked Entity retrieval, an error of type BadRequestData shall be raised.
- If the list of Entity identifiers includes a URI which it is not valid, or the query, geoquery or context source filter are not syntactically valid (as per the referred clauses 4.9 and 4.10) an error of type BadRequestData shall be raised.
- If the ordering parameter is present and the execution of the operation is not limited to the local scope (see clause 5.5.13) then an error of type BadRequestData shall be raised.
- If the ordering parameter is present and refers an entity name other than "id", then an error of type BadRequestData shall be raised.
- If a preferred collation setting is present and it does not conform to a valid ICU collation (see IETF RFC 6067 [36]) then an error of type BadRequestData shall be raised.
- Term to URI expansion of type and Attribute names shall be observed mandated by clause 5.5.7.
- If a list of Attribute names whose values shall be expanded to URIs has been supplied, JSON-LD type coercion shall be performed as mandated by clause 5.5.7.
- The lastN parameter refers to a number, n, of Attribute instances which shall correspond to the last n timestamps (in descending ordering) of the temporal property (by default observedAt) within the concerned temporal interval.


- Otherwise, - Let S be the set of selected Entities i.e. the query result set. - If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, and local scope is not specified, implementations shall run a query that shall return an Entity Array containing all the Entities found locally, that meet all of the following conditions (given the respective parameter is provided): - If id(s) is provided, keep in S only those Entities whose id is equivalent to any of the id(s) passed as a parameter. - If an id pattern is provided, keep in S only those Entities whose id matches the id pattern. - If a selector of Entity Types is provided, keep in S only those Entities whose Entity Type names match the selector of Entity Types. - From S, select only those Entities any of whose Attribute instances (corresponding to the Attributes specified by the query or all if none are specified) match the temporal restrictions imposed by the temporal query (as mandated by clause 4.11); i.e. if the time series, for all the concerned Attributes of an Entity, does not include data corresponding to the temporal query interval, then such Entity shall be removed from S, thus it shall not appear in the final result set. Let S4 be this new subset. - Implementations shall run a query that shall return the Temporal Evolution of the matching Entities (all Entities stored locally, in case only a local scope is specified); the logical steps to select the final result set of Entities, and the Attribute instances included as part of their temporal representation, are enumerated as follows: - If id(s) is provided, keep in S only those Entities whose id is equivalent to any of the id(s) passed as a parameter. - If an id pattern is provided, keep in S only those Entities whose id matches the id pattern. - If a selector of Entity Types is provided, keep in S only those Entities whose Entity Type names match the selector of Entity Types. - From S, select only those Entities any of whose Attribute instances (corresponding to the Attributes specified by the query or all if none are specified) match the temporal restrictions imposed by the temporal query (as mandated by clause 4.11); i.e. if the time series, for all the concerned Attributes of an Entity, does not include data corresponding to the temporal query interval, then such Entity shall be removed from S, thus it shall not appear in the final result set. Let S1 be this new subset. - If a values filter query is provided, from S1, select those Entities whose Attribute instances (during the interval defined by the temporal query) meet the matching conditions specified by the query (as mandated by clause 4.9); i.e. the values filter query shall be checked against all the Attribute instances resulting from the initial filtering performed by the temporal query. Let S2 be this new subset. - If no values filter query is provided, then S2 is equal to S1. - If geoquery is present, from S2, select those Entities whose GeoProperty instances meet the geospatial restrictions imposed by the geoquery (as mandated by clause 4.10); those geospatial restrictions shall be checked against the GeoProperty instances that are within the interval defined by the temporal query. Let S3 be this new subset. - If no geoquery is provided, then S3 is equal to S2. - If the Scope query is present, from S3, select those Entities whose Entity Scope instances match the Scope query (as mandated by clause 4.19, for an example see annex C, clause C.5.16). Let S4 be the new subset. - If no Scope query is provided, then S4 is equal to S3.


- If the ContextBroker implementation supports the use of EntityMaps then: - If the location of a resource holding an EntityMap of matching Entity registrations is present it shall be retrieved: - If the resource cannot be found, or the data has expired, a new EntityMap shall be created. - If the data has not expired, only the retrieved EntityMap shall be used to determine which Context Source Registrations match the Entity ID. - If a flag to return an EntityMap was present in the request, and no EntityMap currently exists, then a new EntityMap shall be created.
- Unless local scope is specified (see clause 5.5.13), for Context Source Registrations that match the query and support the "queryTemporal" operation (see operations and operation groups in clause 4.20), implementations shall do the following: - If an EntityMap is in use for this operation, and an EntityMap entry linked to a Context Source Registration is found, the location of the EntityMap shall be passed as part of the forwarded request. - If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, the filters (filter conditions specified by the query, geospatial restrictions imposed by the geoquery, Scope query, Attributes) shall be removed before forwarding the request. These filters then have to be applied after the Entity information from different Context Sources and local information, if there is any, has been aggregated. - For any exclusive, redirect and inclusive Context Source Registrations that match against the query, the request is forwarded for remote querying by matching endpoints. The result of each remote query is an Entity Array. The returned result is then merged into S4 according to the algorithm defined in clause 4.5.5. - For any auxiliary Context Source Registrations that match against the query, the request is forwarded for remote querying by matching endpoints. Data from the Entity Array received is merged only into S4 when an Attribute instance, whose value of the timeproperty used for the temporal query, is not already present in S4.
- If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, and local scope is not specified, the following filters have to be applied on the aggregated Entities: - If a values filter query is provided, from S4, select those Entities whose Attribute instances (during the interval defined by the temporal query) meet the matching conditions specified by the query (as mandated by clause 4.9); i.e. the values filter query shall be checked against all the Attribute instances resulting from the initial filtering performed by the temporal query. Let S5 be this new subset. - If no values filter query is provided, then S5 is equal to S4. - If geoquery is present, from S5, select those Entities whose GeoProperty instances meet the geospatial restrictions imposed by the geoquery (as mandated by clause 4.10); those geospatial restrictions shall be checked against the GeoProperty instances that are within the interval defined by the temporal query. Let S6 be this new subset. - If no geoquery is provided, then S6 is equal to S5. - If the Scope query is present, from S6, select those Entities whose Entity Scope instances match the Scope query (as mandated by clause 4.19, for an example see annex C, clause C.5.16). Let S7 be the new subset. - If no Scope query is provided, then S7 is equal to S6.

- Otherwise S7 is equal to S4.
- If a datasetId parameter is provided, from S7, for all Entities, filter the Attribute instances based on the datasetIds specified in the parameter, i.e. keep only the Attribute instances whose datasetId is specified. The default Attribute instance is matched, if "@none" is specified. Remove all Attributes without remaining Attribute instances. Let S8 be this new subset.
- If no datasetId is provided then S8 equals to S7.
- From the set of Entities that are in S8, include in their temporal representation only the Attribute instances (up to lastN) corresponding to the query's projection Attributes, or aggregated values of Attribute instances (if aggregated temporal representation is requested), and which meet the temporal, query and geoquery restrictions. If an aggregated temporal representation is requested and any of the requested Attributes is not eligible for at least one of the aggregation methods specified in the request parameters, then an error of type InvalidRequest shall be raised.
- Pagination logic shall be in place as mandated by clause 5.5.9.
- If in the process of obtaining the query result it is necessary to issue a Context Source discovery operation, the same Context Source filter input parameter (if present) shall be propagated.

5.7.4.5 Output Data

A JSON-LD array representing the matching entities as defined by clause 5.2.21 and selected according to the
behaviour described by clause 5.7.4.4.
If Entity ordering is specified (see clause 4.23), then the JSON-LD array returned shall be ordered according to the
member names specified.
If a restrictive list of Entity member names is present, every Entity within the payload body is reduced down to only
contain the defined Entity members.
If an exclusionary list of Entity member names is present, the defined Entity members listed are removed from each
Entity within the payload.

| 5.7.5 | | Retrieve Available Entity Types | |
| --- | --- | --- | --- |
| 5.7.5.1 | | Description | |
| This operation allows retrieving a list of NGSI-LD entity types for which entity instances exist within the NGSI-LD | | | |
| system. | | | |
| 5.7.5.2 | | Use case diagram | |

A Context Consumer can retrieve a list of NGSI-LD entity types from the system as shown in Figure 5.7.5.2-1.


Figure 5.7.5.2-1: Retrieve Available Entity Types use case

# Related

* [HTTP: Resource: temporal/entities/](/http-binding/resource-temporal-entities.md)
* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 4.23](/framework/languages/entity-ordering.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Query Temporal Evolution of Entities
