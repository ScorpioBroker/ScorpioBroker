---
type: NGSI-LD Operation
title: "Create EntityMap for Query Entities"
description: "5.14.4.1 Description This operation is very similar to the Query Entities operation in clause 5.7.2, except that it does not directly return Entities, but creates and returns an Entity map including t"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.14.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.14.4"
---

5.14.4.1 Description

This operation is very similar to the Query Entities operation in clause 5.7.2, except that it does not directly return Entities, but creates and returns an Entity map including the identifiers of the Entities that are candidates to be part of the query results. The Entity map can then be used for paginating through the included Entities.

5.14.4.2 Use case diagram

A Context Consumer can retrieve an Entity map with the Entities that match a specific query from an NGSI-LD system as shown in Figure 5.14.4.2-1.

Figure 5.14.4.2-1: Query Entities for creating EntityMap use case

5.14.4.3 Input data

To simplify the operation, the same parameters as for the Query Entities operation (clause 5.7.2) are allowed, but some of these are irrelevant for creating an Entity map and thus shall be ignored.

- A reference to a JSON-LD @context (optional).
- A selector of Entity types as specified by clause 4.17 (optional). Both type names (short hand string) and fully qualified type names (URI) are allowed in the selector.
- A list (one or more) of Entity identifiers (optional).
- A list (one or more) of Attribute names of which at least one shall exist in order for an Entity to be selected, and also used as query projection attributes (optional).
- A restrictive list of Entity member names ("id", "type", "scope" or an Attribute name) to be retrieved (projection attributes as defined by clause 4.21) (optional).
- An exclusionary list member names ("id", "type", "scope" or an Attribute name) to be removed (projection attributes as defined by clause 4.21) (optional).
- An id pattern as a regular expression (optional).
- An NGSI-LD query (to filter out Entities by Attribute values) as per clause 4.9 (optional).
- An NGSI-LD geoquery (to filter out Entities by spatial relationships) as mandated by clause 4.10 (optional).


- In the case of GeoJSON representation: - The name of the GeoProperty attribute to use as the geometry for the GeoJSON representation as mandated by clause 4.5.16 (ignored). - A datasetId specifying which instance of the value is to be selected if the GeoProperty value has multiple instances as defined by clause 4.5.5 (ignored).
- A NGSI-LD Scope query (to filter out Entities based on their Scope) as mandated by clause 4.19 (optional).
- An NGSI-LD query (called context source filter, to filter out Context Sources by the values of properties that describe them) as per clause 4.9 (optional).
- A limit to the number of Entities to be retrieved. See clause 5.5.9 (ignored).
- A specified language filter as per clause 4.15 (optional).
- A list (one or more) of Attribute names whose values shall be expanded to URIs prior to executing a query (optional).
- An optional flag indicating whether to include additional Linked Entities corresponding to the Relationships retrieved and how to format those Linked Entities. See clause 4.5.23 (optional).
- A limit to the depth of Linked Entities to search whilst traversing an Entity Graph. See clause 4.5.23 (optional).
- A list (one or more) of Linked Entity identifiers previously encountered whilst traversing an Entity Graph. See clause 4.5.23 (optional).
- A flag indicating whether to return the location of the EntityMap used within the operation (ignored, EntityMap is always returned).
- A suggested expiry time for the EntityMap (optional).
- The location of a resource holding an EntityMap of matching Entity registrations (ignored).
- A datasetId parameter that specifies which Attribute instances are to be selected as defined by clause 4.5.5 (optional).
- A flag indicating whether split Entities are to be expected, i.e. Entities whose information is distributed across different Context Sources (optional). In the general case, it is not possible to retrieve a set of entities by only specifying desired Entity identifiers, without further specifying restrictions on the Entities' types or attributes, either explicitly, via selector of Entity types or of Attribute names, or implicitly, within an NGSI-LD Query or GeoQuery. If the execution of the operation is limited to the local scope (see clause 5.5.13), no further restrictions have to be provided.

5.14.4.4 Behaviour

- At least one of the following input data shall be provided: a) selector of Entity Types; b) list of Attribute names, including at least one non-system Attribute; c) NGSI-LD Query, including at least one non-system Attribute; d) NGSI-LD GeoQuery; e) local scope (see clause 5.5.13). If none of the above is provided, then an error of type BadRequestData shall be raised (too wide query).


- If the list of Entity identifiers includes a URI which it is not valid, or the query, geoquery or context source filter are not syntactically valid (as per the referred clauses 4.9 and 4.10) an error of type BadRequestData shall be raised.
- If projection attributes are present and indicate the use of Linked Entity retrieval, and the use of Linked Entity retrieval is not specified, or the projected attribute depth exceeds the Linked Entity retrieval depth, then an error of type BadRequestData shall be raised.
- If the filter conditions specified by the query includes Linked Entity attributes, and the use of Linked Entity retrieval is not specified, or the Linked Entity attribute query depth exceeds the Linked Entity retrieval depth, an error of type BadRequestData shall be raised (too deep query).
- If geometryProperty parameter is present and the Accept Header is not set to "application/geo+json", then an error of type BadRequestData shall be raised.
- Otherwise, - Term to URI expansion of type and Attribute names shall be performed, as mandated by clause 5.5.7. - If a list of Attribute names whose values shall be expanded to URIs has been supplied, JSON-LD type coercion shall be performed as mandated by clause 5.5.7. - If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, and local scope is not specified, implementations shall run a query that shall return an EntityMap containing the identifiers of all the Entities found locally that meet all of the following conditions: - id is equal to any of the id(s) passed as a parameter; - the Entity Type names match the selector of Entity Types (expanded) that is passed as a parameter; - id matches the id pattern passed as a parameter. - Otherwise, implementations shall run a query that shall return an EntityMap containing the identifiers pf all Entities found locally that meet all of the following conditions (given the respective parameter is provided): - id is equal to any of the id(s) passed as a parameter; - the Entity Type names match the selector of Entity Types (expanded) that is passed as a parameter; - Attribute matches any of the expanded attribute(s) in the list that is passed as a parameter; - id matches the id pattern passed as a parameter; - the filter conditions specified by the query are met (as mandated by clause 4.9); - the geospatial restrictions imposed by the geoquery are met (as mandated by clause 4.10); if there are multiple instances of the GeoProperty on which the geoquery is based, it is sufficient if any of these instances meets the geospatial restrictions; - if the Scope query is present, it shall match a present Entity Scope (as mandated by clause 4.19, for an example see annex C, clause C.5.15); - if the Attribute list is present, in order for an Entity to match, it shall contain at least one of the Attributes in the projection Attribute list.
- Unless local scope is specified (see clause 5.5.13), for Context Source Registrations that match the query and support the "createEntityMapQueryEntity" operation (see operations and operation groups in clause 4.20), implementations shall do the following: - If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, the filters (filter conditions specified by the query, geospatial restrictions imposed by the geoquery, Scope query, Attributes) shall be removed before forwarding the request.


- For each matching Context Source Registration, the request is forwarded for remote querying by matching endpoints. The result of each remote query is an EntityMap. The mapping between the Context Source Registration and the EntityMap Id is added to the linkedMaps element of the local EntityMap and for the Entity ids included in the returned EntityMaps a mapping to the Context Source Registration is added to the entityMap element of the local EntityMap.

- The local EntityMap is stored and made accessible based on its identifier.

5.14.4.5 Output data

The location of the local EntityMap shall be returned in a specific field in the response, as well as the EntityMap itself.

# Related

* [HTTP: Resource: entityMaps](/http-binding/resource-entitymaps.md)
* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 4.5.16](/framework/data-representation/geojson-representation-of-entities.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.14.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Create EntityMap for Query Entities
