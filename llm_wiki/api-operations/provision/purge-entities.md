---
type: NGSI-LD Operation
title: "Purge Entities"
description: "5.6.21.1 Description This operation allows the deletion of entities within an NGSI-LD system based upon a query."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.6.21
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.6.21"
---

5.6.21.1 Description

This operation allows the deletion of entities within an NGSI-LD system based upon a query.

5.6.21.2 Use case diagram

A Context Producer can delete a set of entities which matches a specific query from an NGSI-LD system as shown in Figure 5.6.21.2-1.


Figure 5.6.21.2-1: Purge Entities use case

5.6.21.3 Input data

- A reference to a JSON-LD @context (optional).
- A selector of Entity types as specified by clause 4.17 (optional). Both type names (short hand string) and fully qualified type names (URI) are allowed in the selector.
- A list (one or more) of Entity identifiers (optional).
- A restrictive list of Attribute names to be deleted as attributes (optional).
- An exclusionary list Attribute names to be kept as attributes (optional).
- An id pattern as a regular expression (optional).
- An NGSI-LD query (to filter out Entities by Attribute values) as per clause 4.9 (optional).
- An NGSI-LD geoquery (to filter out Entities by spatial relationships) as mandated by clause 4.10 (optional).
- A NGSI-LD Scope query (to filter out Entities based on their Scope) as mandated by clause 4.19 (optional).
- An NGSI-LD query (called context source filter, to filter out Context Sources by the values of properties that describe them) as per clause 4.9 (optional). In the general case, it is not possible to purge a set of entities by only specifying desired Entity identifiers, without further specifying restrictions on the entities' types or attributes, either explicitly, via selector of Entity types or of Attribute names, or implicitly, within an NGSI-LD Query or GeoQuery. If the execution of the operation is limited to the local scope (see clause 5.5.13), no further restrictions have to be provided.

5.6.21.4 Behaviour

- At least one of the following input data shall be provided: a) selector of Entity Types; b) list of Attribute names, including at least one non-system Attribute; c) NGSI-LD Query, including at least one non-system Attribute; d) NGSI-LD GeoQuery; e) local scope (see clause 5.5.13). If none of the above is provided, then an error of type BadRequestData shall be raised (too wide query).


- If the list of Entity identifiers includes a URI which it is not valid, or the query, geoquery or context source filter are not syntactically valid (as per the referred clauses 4.9 and 4.10) an error of type BadRequestData shall be raised.
- If projection attributes are present and indicate the use of Linked Entity retrieval, then an error of type BadRequestData shall be raised.
- If the filter conditions specified by the query includes Linked Entity attributes then an error of type BadRequestData shall be raised.
- Term to URI expansion of type and Attribute names shall be performed, as mandated by clause 5.5.7.
- Otherwise, implementations shall run a Query Entities operation (clause 5.6.2) to retrieve a list of ids of Entities for deletion, that meet all of the following conditions (given the respective parameter is provided): - id is equal to any of the id(s) passed as a parameter; - the Entity Type names match the selector of Entity Types (expanded) that is passed as a parameter; - Attribute matches any of the expanded attribute(s) in the list that is passed as a parameter; - id matches the id pattern passed as a parameter; - the filter conditions specified by the query are met (as mandated by clause 4.9); - the geospatial restrictions imposed by the geoquery are met (as mandated by clause 4.10); if there are multiple instances of the GeoProperty on which the geoquery is based, it is sufficient if any of these instances meets the geospatial restrictions; - if the Scope query is present, it shall match a present Entity Scope (as mandated by clause 4.19, for an example see annex C, clause C.5.15). And thereafter: - when no restrictive list of Entity member names is present, the implementation shall delete all Entities that can be found locally using retrieved list of Entity ids; - when a restrictive list of Entity member names is present, the implementation shall delete the given set of Attributes with those member names from all Entities that can be found locally using retrieved list of Entity ids; - when an exclusionary list of Entity member names is present, the implementation shall delete all but the given set of Attributes with those member names from all Entities that can be found locally using retrieved list of Entity ids.
- Unless local scope is specified (see clause 5.5.13), for Context Source Registrations that match the query and support the "purgeEntity" operation (see operations and operation groups in clause 4.20), implementations shall do the following:
- If an inclusive, exclusive or redirect Context Source Registration matches against the filter conditions specified, the request is forwarded for remote processing. For each matching registration: - If the Purge Entity operation is supported by the registration (see clause 4.3.6), matching input data is forwarded to the Registration endpoint. - If the Purge Entity operation is not supported by the matched registration, this shall result in an error of type Conflict in case the complete purge Entities failed, or in a partial success if some parts of purge Entities succeeded.

5.6.21.5 Output data

None.

# Related

* [HTTP: Resource: entities/](/http-binding/resource-entities.md)
* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.3.6](/framework/architecture/distributed-operations.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.5.13](/api-operations/common-behaviours/limiting-operations-to-local-scope.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.6.21](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Purge Entities
