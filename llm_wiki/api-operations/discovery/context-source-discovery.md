---
type: NGSI-LD Clause
title: "Context Source Discovery"
description: "5.10.1 Retrieve Context Source Registration 5.10.1.1 Description This operation allows retrieving a specific context source registration from an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.10
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.10"
---

5.10.1 Retrieve Context Source Registration

5.10.1.1 Description

This operation allows retrieving a specific context source registration from an NGSI-LD system.

5.10.1.2 Use case diagram

A context consumer or a context provider can retrieve a specific context source registration from an NGSI-LD system as shown in Figure 5.10.1.2-1.

Figure 5.10.1.2-1: Retrieve context source registration use case

5.10.1.3 Input data

Context source registration identifier (id) of the context source registration to be retrieved (target registration).

5.10.1.4 Behaviour

- If the context source registration id (id) is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about the target context source registration, because there is no existing context source registration whose id (URI) is equivalent, then an error of type ResourceNotFound shall be raised.
- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- Otherwise return a JSON-LD object representing the Context Source Registration as mandated by clause 5.2.9.


5.10.1.5 Output data

A JSON-LD object representing the target context source registration as mandated by clause 5.2.9.

5.10.2 Query Context Source Registrations

5.10.2.1 Description

This operation allows discovering context source registrations from an NGSI-LD system. The behaviour of the
discovery of context source registrations differs significantly from the querying of entities as described in clause 5.7.2.
The approach is that the client submits a query for entities as described in clause 5.7.2, but instead of receiving the
Entity information, it receives a list of Context Source Registrations describing Context Sources that
possibly have some of the requested Entity information. This means that the requested Entities and Attributes are
matched against the 'information' property as described in clause 5.12.
If no temporal query is present, only Context Source Registrations for Context Sources providing
latest information, i.e. without specified time intervals, are considered. If a temporal query is present only Context
Source Registrations with matching time intervals, i.e. observationInterval or managementInterval, are
considered.

5.10.2.2 Use case diagram

A Context Consumer can discover context source registrations that may be able to provide (part of) the context information specified in the query from an NGSI-LD system as shown in Figure 5.10.2.2-1.

Figure 5.10.2.2-1: Discover context source registrations use case

5.10.2.3 Input data

- A reference to a JSON-LD @context (optional).
- A selector of Entity types as specified by clause 4.17. Both type name (short hand string) and fully qualified type name (URI) are allowed (optional).
- A list (one or more) of Entity identifiers (optional).
- A list (one or more) of Attribute names (called query projection attributes) (optional).
- An id pattern as a regular expression (optional).

Context Consumer

Discover Context Source Registrations

NGSI-LD Client

NGSI-LD System

Response with List of Context Source Registrations

1..*

CSourceRegistration Array

discover context source registrations


- An NGSI-LD query (to filter out Entities by Attribute values, used here to identify relevant attributes) as per clause 4.9 (optional).
- An NGSI-LD geoquery (to filter out Entities by spatial relationships, used here to identify relevant GeoProperties and for geographical scoping) as per clause 4.10 (optional).
- In the case of GeoJSON representation: - The name of the GeoProperty attribute to use as the geometry for the GeoJSON representation as mandated by clause 4.5.16 (optional). - A datasetId specifying which instance of the value is to be selected if the GeoProperty value has multiple instances as defined by clause 4.5.5 (optional).
- An NGSI-LD temporal query as per clause 4.11 (optional).
- An NGSI-LD context source query as per clause 4.9 (optional).
- A NGSI-LD Scope query as mandated by clause 4.19 (optional).
- A limit to the number of Context Source Registrations to be retrieved. See clause 5.5.9.
- A specified language filter as per clause 4.15 (optional). It is not possible to retrieve a set of context source registrations related to entities by only specifying desired Entity identifiers, without further specifying restrictions on the entities' types or attributes, either explicitly, via lists of Entity types or of Attribute names, or implicitly, within an NGSI-LD query or geoquery.

5.10.2.4 Behaviour

- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- At least one of the following input data shall be provided: a) selector of Entity Types; b) list of Attribute names; c) NGSI-LD Query; d) NGSI-LD GeoQuery. If none of them is provided, then an error of type BadRequestData shall be raised (too wide query). Attributes specified in NGSI-LD Query or NGSI-LD GeoQuery shall be used for matching RegistrationInfo elements in the same way as the attributes in the list of Attribute names.
- If the list of Entity identifiers includes a URI which it is not valid, or the query, geoquery or temporal query are not syntactically valid (as per clauses 4.9, 4.10 and 4.11) an error of type BadRequestData shall be raised.
- Term to URI expansion of type and Attribute names shall be performed, as mandated by clause 5.5.7.
- Otherwise, implementations shall run a query that shall return context source registrations that meet all the applicable conditions: - If present, the entity specification in the query consisting of a combination of entity type selector and entity id/entity id pattern (optional) matches an EntityInfo specified in a RegistrationInfo of the information property in a context source registration. If there is no EntityInfo specified in the RegistrationInfo, the entity specification is considered matching. This matching is further described in clause 5.12. - If present, at least one Attribute name specified in the query matches one Property or Relationship in the RegistrationInfo element of the information property in a context source registration. If no Properties or Relationships are specified in the RegistrationInfo, the Attribute names are considered matching. This matching is further described in clause 5.12.


- If present, the geoquery is matched against the GeoProperty identified in the geoquery. If no
GeoProperty is specified in the geoquery, the default property is location. The geoquery matches the
GeoProperty specified in the Context Source Registration, if the location directly matches or
if the location possibly contains locations that would match the geoquery.
- If no temporal query is present, only Context Source Registrations for Context
Sources providing latest information, i.e. without specified time intervals, are considered.
- If a temporal query is present, only Context Source Registrations with specified time
intervals, i.e. observationInterval or managementInterval are considered. If the timeproperty is
observedAt or no timeproperty is specified in the temporal query (default: observedAt), the temporal
query is matched against the observationInterval (if present). If the timeproperty is createdAt, modifiedAt
or deletedAt, the temporal query is matched against the managementInterval (if present). If the relevant
interval is not present, there is no match:
- The semantics of the match is that the timeAt in the case of the "before" and "after" relation
is contained in or is an endpoint of a time period included in the specified time interval. In the case
of the "between" relation there is a match if there is an overlap between the interval specified by
the timeAt and endTimeAt and the specified time interval.
- If present, the conditions specified by the context source query filter match the respective Context
Source Properties (as mandated by clause 4.9).
- If present, the Scope query (as mandated by clause 4.19) is matched against the scope property.
- Pagination logic shall be in place as mandated by clause 5.5.9.

5.10.2.5 Output data

A JSON-LD array of matching Context Source Registrations as defined by clause 5.2.9. Instead of the original Context Source Registration which may contain a lot of irrelevant information, implementations should return filtered Context Source Registrations, which only contain context source registration information relevant for the query, in particular only matching RegistrationInfo elements.

# Related

* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.15](/framework/languages/ngsi-ld-language-filter.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.5.16](/framework/data-representation/geojson-representation-of-entities.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Context Source Discovery
