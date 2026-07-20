---
type: NGSI-LD Clause
title: "Context Information Consumption"
description: "5.7.1 Retrieve Entity 5.7.1.1 Description This operation allows retrieving an NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7"
---

5.7.1 Retrieve Entity

5.7.1.1 Description

This operation allows retrieving an NGSI-LD Entity.

5.7.1.2 Use case diagram

A Context Consumer can retrieve a specific Entity from an NGSI-LD system as shown in Figure 5.7.1.2-1.

Figure 5.7.1.2-1: Retrieve Entity use case

5.7.1.3 Input data

- Entity ID (URI) of the Entity to be retrieved (target Entity).
- A selector of Entity types as specified by clause 4.17 (optional).
- List of Attribute (Properties or Relationships) names to be retrieved (projection attributes) (optional).
- A restrictive list of Entity member names ("id", "type", "scope" or an Attribute name) to be retrieved (projection attributes as defined by clause 4.21) (optional).
- An exclusionary list of Entity member names ("id", "type", "scope" or an Attribute name) to be removed (projection attributes as defined by clause 4.21) (optional).
- A language filter as defined by clause 4.15 (optional).
- An optional JSON-LD context.
- In the case of a GeoJSON representation, the name of the GeoProperty attribute to use as the geometry for the GeoJSON representation as mandated by clause 4.5.16 (optional).

Context Consumer

Retrieve Entity

NGSI-LD Client

NGSI-LD System

Response with (Entity)

1..*

Entity

retrieve entity (entityId)

Response with (GeoJSON Feature)

GeoJSON Feature


- An optional flag indicating whether to include additional Linked Entities corresponding to the Relationships retrieved and how to format those Linked Entities. See clause 4.5.23 (optional).
- A limit to the depth of Linked Entities to search whilst traversing an Entity graph. See clause 4.5.23 (optional).
- A list (one or more) of Linked Entity identifiers previously encountered whilst traversing an Entity graph. See clause 4.5.23 (optional).
- A flag indicating whether to return the location of the EntityMap used within the operation (optional).
- A suggested lifetime for the EntityMap, if EntityMap is to be created (optional).
- The location of a resource holding an EntityMap of matching Entity registrations (optional).
- A datasetId parameter that specifies which Attribute instances are to be selected as defined by clause 4.5.5 (optional).

5.7.1.4 Behaviour

- If the Entity ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If geometryProperty parameter is present and the Accept Header is not set to "application/geo+json", then an error of type BadRequestData shall be raised.
- If projection attributes are present and indicate the use of Linked Entity retrieval and the use of Linked Entity retrieval is not specified, or the projected attribute depth exceeds the Linked Entity retrieval depth, an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target Entity, because there is no existing Entity held locally whose id (URI), and where specified type, is equivalent, and no matching registrations apply, then an error of type ResourceNotFound shall be raised.
- The implementation shall retrieve any Attribute data held locally which is associated with the Entity defined by the Entity ID.
- If the ContextBroker implementation supports the use of EntityMaps then: - If the location of a resource holding an EntityMap of matching Entity registrations is present it shall be retrieved: - If the resource cannot be found, or the data has expired, a new EntityMap shall be created. - If the data has not expired, only the retrieved Entity Map shall be used to determine which Context Source Registrations match the Entity ID. - If a flag to return an EntityMap was present in the request, and no EntityMap currently exists, then a new EntityMap shall be created.
- For Context Source Registrations that match and support the retrieveEntity operation (see operations and operation groups in clause 4.20), implementations shall do the following: - If an EntityMap is in use for this operation, and an EntityMap entry linked to a Context Source Registration is found, the location of the linked EntityMap shall be passed as part of any forwarded request. - For any exclusive, redirect and inclusive Context Source Registrations, the request is forwarded for remote retrieval by matching endpoints, and remote Attribute data for the Entity is received. If an expiresAt DateTime is present on the Entity and the date lies in the past, the Attribute data shall be discarded, otherwise the Attribute data is then merged together according to the algorithm defined in clause 4.5.5. - For any auxiliary Context Source Registrations the remote Attribute data received is added to the payload only when an Attribute is not present in any of the Attribute data received elsewhere.


- If an EntityMap is in use for this operation, the EntityMap's linked maps are updated to hold the location of every EntityMap used by the Context Source Registrations.

- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- For each Attribute found in the target Entity, when the datasetId parameter is provided in the request: - Filter the Attribute instances based on the datasetId parameter, i.e. keep only the Attribute instances whose datasetId is specified. The default Attribute instance is matched, if "@none" is specified. - If there is no Attribute instance whose datasetId matches the value of the parameter, the Attribute shall not be returned with the Entity.
- If the optional Attribute list is present and the NGSI-LD endpoint does know about a matching Entity for the Entity ID, but this Entity does not have any of the Attributes in the Attribute list, then an error of type ResourceNotFound shall be raised.
- If the Accept Header is set to "application/json" or "application/ld+json", return a JSON LD object representing the Entity as mandated by clause 5.2.4 and containing only the Attributes requested (if present). - If the Prefer Header [26] is set to "ngsi-ld= " then the ContextBroker shall endeavour to amend the JSON-LD object to conform to the specified version of the NGSI-LD specification as mandated in clause 4.3.6.8, and return a Preference-Applied Header set to "ngsi-ld= " in the response.
- If the Accept Header is set to "application/geo+json", a GeoJSON Feature object representing the entity as mandated by clause 5.2.29 and containing only the Attributes requested (if present): - If the Prefer Header is omitted or set to "body=ld+json" then the Feature object will also contain an @context field. - If the Prefer Header is set to "body=json" the @context is set as a Link Header and removed from the Feature object.

5.7.1.5 Output data

A JSON-LD object representing the target Entity as mandated by clause 5.2.4 or a GeoJSON Feature as mandated by
clause 5.2.29.
If a restrictive list of Entity member names is present, the Entity is reduced down to only contain the defined Entity
members (applies to all Entities in the payload in the case of Linked Entity retrieval).
If an exclusionary list of Entity member names is present, the defined Entity members listed are removed from the
Entity (applies to all Entities in the payload in the case of Linked Entity retrieval).
If any of the returned Attributes corresponds to a VocabProperty, the returned value shall be compacted according
to the supplied @context.
If a language filter is specified and any of the returned Attributes corresponds to a LanguageProperty, the
LanguageProperty in question shall be converted into a Property. The value of this Property shall correspond to the
value of the string or strings from the matching key-value pair of the languageMap where the key matches the language
filter. A non-reified subproperty lang shall be included in the response indicating the chosen language.
If no match can be made for a LanguageProperty then a single language shall be chosen, up to the implementation.
If inline Linked Entity retrieval (see clause 4.5.23.2) is specified, and any of the returned Attributes corresponds
to an annotated Relationship, then unless a URI has been previously encountered, an entity sub-Property shall be
included in the response holding the Linked Entity data for each URI corresponding to that Relationship's target
object URI. If any of the returned Attributes corresponds to an annotated ListRelationship, then an entityList
subproperty shall be included in the response holding the ordered array of Linked Entities corresponding to that
ListRelationship's target objectList URIs unless a URI has been previously encountered.

If flattened Linked Entity retrieval (see clause 4.5.23.3) is specified, the response shall be an array of JSON-LD
objects representing the target entity itself and the targets of its Relationships. If any of the returned Attributes
corresponds to an annotated Relationship, unless a target URI has been previously encountered, the data corresponding
to each URI of the Relationship's target object URIs is appended to the array. If any of the returned Attributes
corresponds to an annotated ListRelationship, then an ordered array of additional Linked Entities is appended to
the array which hold the data corresponding to each of the URIs found within ListRelationship's target objectList unless
a URI has been previously encountered.
Flattened Linked Entity retrieval output changes to a GeoJSON FeatureCollection as mandated by clause 5.2.30 if
the Accept Header is set to "application/geo+json".

If the location of a previously generated EntityMap was passed into the request, or a flag to return an EntityMap was present in the request, the location of the EntityMap used in the operation shall be returned in a specific field in the response.

| response. | | | | |
| --- | --- | --- | --- | --- |
| 5.7.2 | | Query Entities | | |
| 5.7.2.1 | | Description | | |

This operation allows querying an NGSI-LD system.

5.7.2.2 Use case diagram

A Context Consumer can retrieve a set of entities which matches a specific query from an NGSI-LD system as shown in Figure 5.7.2.2-1.

Query Entities

1..*

Context Consumer

NGSI-LD Client

NGSI-LD System

query entities (query)

Response with (Entity List)

Entity Array

Response with (GeoJSON FeatureCollection)

GeoJSON FeatureCollection

Figure 5.7.2.2-1: Query Entities use case

5.7.2.3 Input data

- A reference to a JSON-LD @context (optional).
- A selector of Entity types as specified by clause 4.17 (optional). Both type names (short hand string) and fully qualified type names (URI) are allowed in the selector.


- A list (one or more) of Entity identifiers (optional).
- An id pattern as a regular expression (optional).
- An NGSI-LD query (to filter out Entities by Attribute values) as per clause 4.9 (optional).
- An NGSI-LD geoquery (to filter out Entities by spatial relationships) as mandated by clause 4.10 (optional).
- A NGSI-LD Scope query (to filter out Entities based on their Scope) as mandated by clause 4.19 (optional).
- A restrictive list of Entity member names ("id", "type", "scope" or an Attribute name) to be retrieved (projection attributes as defined by clause 4.21) (optional).
- An exclusionary list member names ("id", "type", "scope" or an Attribute name) to be removed (projection attributes as defined by clause 4.21) (optional).
- A list (one or more) of Attribute names of which at least one shall exist in order for an Entity to be selected, and also used as query projection attributes (optional, deprecated).
- An NGSI-LD query (called context source filter, to filter out Context Sources by the values of properties that describe them) as per clause 4.9 (optional).
- A limit to the number of Entities to be retrieved. See clause 5.5.9.
- A specified language filter as per clause 4.15 (optional).
- A list (one or more) of Attribute names whose values shall be expanded to URIs prior to executing a query (optional).
- An optional flag indicating whether to include additional Linked Entities corresponding to the Relationships retrieved and how to format those Linked Entities. See clause 4.5.23 (optional).
- A limit to the depth of Linked Entities to search whilst traversing an Entity Graph. See clause 4.5.23 (optional).
- A list (one or more) of Linked Entity identifiers previously encountered whilst traversing an Entity Graph. See clause 4.5.23 (optional).
- A flag indicating whether to return the location of the EntityMap used within the operation (optional).
- A suggested lifetime for the EntityMap, if EntityMap is to be created (optional).
- The location of a resource holding an EntityMap of matching Entity registrations (optional).
- A datasetId parameter that specifies which Attribute instances are to be selected as defined by clause 4.5.5 (optional).
- A list of Entity member names ("id", "type", "scope" or an Attribute name) to be used to define the preferred ordering of Entities retrieved (Entity ordering attributes as defined by clause 4.23) (optional).
- A preferred collation setting to be used when applying ordering of Entities (optional).
- A location to be used when applying ordering of Entities (optional).
- A defined geometry type to be used when applying ordering of Entities (optional).
- A flag indicating whether split Entities are to be expected, i.e. Entities whose information is distributed across different Context Sources (optional).
- In the case of GeoJSON representation, the name of the GeoProperty attribute to use as the geometry for the GeoJSON representation as mandated by clause 4.5.16 (optional).


In the general case, it is not possible to retrieve a set of entities by only specifying desired Entity identifiers, without further specifying restrictions on the entities' types or attributes, either explicitly, via selector of Entity types or of Attribute names, or implicitly, within an NGSI-LD Query or GeoQuery. If the execution of the operation is limited to the local scope (see clause 5.5.13), no further restrictions have to be provided.

5.7.2.4 Behaviour

- At least one of the following input data shall be provided: a) selector of Entity Types; b) list of Attribute names, including at least one non-system Attribute; c) NGSI-LD Query, including at least one non-system Attribute; d) NGSI-LD GeoQuery; e) local scope (see clause 5.5.13). If none of the above is provided, then an error of type BadRequestData shall be raised (too wide query).
- If the list of Entity identifiers includes a URI which it is not valid, or the query, geoquery or context source filter are not syntactically valid (as per the referred clauses 4.9 and 4.10) an error of type BadRequestData shall be raised.
- If projection attributes are present and indicate the use of Linked Entity retrieval, and the use of Linked Entity retrieval is not specified, or the projected attribute depth exceeds the Linked Entity retrieval depth, then an error of type BadRequestData shall be raised.
- If the filter conditions specified by the query includes Linked Entity attributes, and the use of Linked Entity retrieval is not specified, or the Linked Entity attribute query depth exceeds the Linked Entity retrieval depth, an error of type BadRequestData shall be raised (too deep query).
- If geometryProperty parameter is present and the Accept Header is not set to "application/geo+json", then an error of type BadRequestData shall be raised.
- If the ordering parameter is present and the execution of the operation is not limited to the local scope (see clause 5.5.13) then an error of type BadRequestData shall be raised.
- If the ordering parameter is present and refers to ordering by distance and no location is present, then an error of type BadRequestData shall be raised.
- If a preferred collation setting is present and it does not conform to a valid ICU collation (see IETF RFC 6067 [36]) then an error of type BadRequestData shall be raised.
- If a location to be used when applying ordering of Entities setting is present and it is not syntactically valid (as per the referred clauses 4.9 and 4.10) or an error of type BadRequestData shall be raised.
- Otherwise, - Term to URI expansion of type and Attribute names shall be performed, as mandated by clause 5.5.7. - If a list of Attribute names whose values shall be expanded to URIs has been supplied, JSON-LD type coercion shall be performed as mandated by clause 5.5.7.
- If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, and local scope is not specified, implementations shall run a query that shall return an Entity Array containing all the Entities found locally, that meet all of the following conditions (given the respective parameter is provided): - id is equal to any of the id(s) passed as a parameter; - the Entity Type names match the selector of Entity Types (expanded) that is passed as a parameter; - id matches the id pattern passed as a parameter.


- Otherwise, implementations shall run a query that shall return an Entity Array containing all the Entities found locally, that meet all of the following conditions (given the respective parameter is provided): - id is equal to any of the id(s) passed as a parameter; - the Entity Type names match the selector of Entity Types (expanded) that is passed as a parameter; - Attribute matches any of the expanded attribute(s) in the list that is passed as a parameter; - id matches the id pattern passed as a parameter; - the filter conditions specified by the query are met (as mandated by clause 4.9); - the geospatial restrictions imposed by the geoquery are met (as mandated by clause 4.10); if there are multiple instances of the GeoProperty on which the geoquery is based, it is sufficient if any of these instances meets the geospatial restrictions; - if the Scope query is present, it shall match a present Entity Scope (as mandated by clause 4.19, for an example see annex C, clause C.5.15); - if the Attribute list is present, in order for an Entity to match, it shall contain at least one of the Attributes in the projection Attribute list.
- If the ContextBroker implementation supports the use of EntityMaps then: - If the location of a resource holding an EntityMap of matching Entity registrations is present it shall be retrieved: - If the resource cannot be found, or the data has expired, a new EntityMap shall be created. - If the data has not expired, only the retrieved EntityMap shall be used to determine which Context Source Registrations match the Entity ID. - If a flag to return an EntityMap was present in the request, and no EntityMap currently exists, then a new EntityMap shall be created.
- Unless local scope is specified (see clause 5.5.13), for Context Source Registrations that match the query and support the "queryEntity" operation (see operations and operation groups in clause 4.20), implementations shall do the following: - If an EntityMap is in use for this operation, and an EntityMap entry linked to a Context Source Registration is found, the location of the EntityMap shall be passed as part of the forwarded request. - If split entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split entities, the filters (filter conditions specified by the query, geospatial restrictions imposed by the geoquery, Scope query, Attributes) shall be removed before forwarding the request. These filters then have to be applied after the Entity information from different Context Sources and local information, if there is any, has been aggregated. - For any exclusive, redirect and inclusive Context Source Registrations, the request is forwarded for remote querying by matching endpoints. The result of each remote query is an Entity Array. Within each Entity Array, any Entities where an expiresAt DateTime is present and the date lies in the past shall be discarded. The Entity Arrays are then merged together with the locally queried result according to the algorithm defined in clause 4.5.5. - For any auxiliary Context Source Registrations, the request is forwarded for remote querying by matching endpoints. Data from the Entity Array received is added to the payload only when an Attribute is not already present in the merged Entity Arrays are received elsewhere.
- If split Entities flag is explicitly set to true or, if not explicitly set, the default setting of the deployment allows split Entities, and local scope is not specified, the following filters shall be applied on the aggregated Entities: - the filter conditions specified by the query are met (as mandated by clause 4.9);


- the geospatial restrictions imposed by the geoquery are met (as mandated by clause 4.10); if there are
multiple instances of the GeoProperty on which the geoquery is based, it is sufficient if any of these
instances meets the geospatial restrictions;
- if the Scope query is present, it shall match a present Entity Scope (as mandated by clause 4.19, for an
example see annex C, clause C.5.15);
- if the Attribute list is present, in order for an Entity to match, it shall contain at least one of the Attributes
in the projection Attribute list.
- Pagination logic shall be in place as mandated by clause 5.5.9.
- If in the process of obtaining the query result it is necessary to issue a Context Source discovery operation, the same Context Source filter input parameter (if present) shall be propagated.
- For each Attribute found in the target Entity, when datasetId parameter is provided in the request: - Filter the Attribute instances based on the datasetId parameter, i.e. keep only the Attribute instances whose datasetId is specified. The default Attribute instance is matched, if "@none" is specified. - If there is no Attribute instance whose datasetId matches the value of the parameter, the Attribute shall not be returned with the Entity.
- If the Accept Header is set to "application/json" or "application/ld+json", a JSON-LD array is returned, representing the Entities as mandated by clause 5.2.4 and containing only the Attributes requested (if present). - If the Prefer Header [26] is set to "ngsi-ld= " then the ContextBroker shall endeavour to amend the elements of the JSON-LD array to conform to the specified version of the NGSI-LD specification as mandated in clause 4.3.6.8, and return a Preference-Applied Header set to "ngsi-ld= " in the response.
- If the Accept Header is set to "application/geo+json", the response shall be a GeoJSON FeatureCollection as mandated by clause 5.2.30, with each Feature within the FeatureCollection containing only the Attributes requested (if present): - If the Prefer Header is omitted or set to "body=ld+json" then the FeatureCollection will also contain an @context field. - If the Prefer Header is set to "body=json" the @context is sent as a Link Header and removed from the FeatureCollection object.

5.7.2.5 Output data

A JSON-LD array representing the matching entities as defined by clause 5.2.4 or in the case of GeoJSON requests a
FeatureCollection as mandated by clause 5.2.30. If Entity ordering is specified (see clause 4.23), then the JSON-LD
array or FeatureCollection returned shall be ordered according to the list of member names specified. For each
matching Entity, only the Attributes specified by the Attribute list parameter shall be included. If such parameter is not
present, then all Attributes shall be included.
If a restrictive list of Entity member names is present, every Entity within the payload body is reduced down to only
contain the defined Entity members.
If an exclusionary list of Entity member names is present, the defined Entity members listed are removed from each
Entity within the payload.
If any of the returned Attributes corresponds to a VocabProperty, the returned value shall be compacted according
to the supplied @context.
If a language filter is specified and any of the returned Attributes corresponds to a LanguageProperty, the
LanguageProperty in question shall be converted into a Property. The value of this Property shall correspond to the
value of the string or strings from matching key-value pair of the languageMap where the key matches the language
filter. A non-reified subproperty lang shall be included in the response indicating the chosen language.


If no match can be made for a LanguageProperty, then the default identified by the JSON-LD "@none" shall be
chosen if present, otherwise the choice of a single language is up to the implementation.
If inline Linked Entity retrieval (see clause 4.5.23.2) is specified, and any of the returned Attributes corresponds
to an annotated Relationship, then unless a URI has been previously encountered, an entity subproperty shall also be
included in the response holding the data for each URI corresponding to that Relationship's target object URIs. If any of
the returned Attributes corresponds to an annotated ListRelationship, then an entityList subproperty shall also be
included in the response holding the ordered data corresponding to that L

… (truncated; see full clause in the PDF).

# Related

* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 4.23](/framework/languages/entity-ordering.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Information Consumption
