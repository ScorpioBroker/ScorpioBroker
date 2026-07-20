---
type: NGSI-LD Operation
title: "Retrieve Entity"
description: "5.7.1.1 Description This operation allows retrieving an NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.7.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.7.1"
---

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

# Related

* [HTTP: Resource: entities/{entityId}](/http-binding/resource-entities-entityid.md)
* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.20](/framework/languages/ngsi-ld-distributed-operation-names.md)
* [Clause 4.21](/framework/languages/ngsi-ld-attribute-projection-language.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.16](/framework/data-representation/geojson-representation-of-entities.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
* [Clause 4.5.23.2](/framework/data-representation/inline-linked-entity-representation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.7.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Entity
