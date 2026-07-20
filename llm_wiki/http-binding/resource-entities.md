---
type: NGSI-LD Resource
title: "Resource: entities/"
description: "6.4.1 Description This resource represents the entities known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.4
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.4"
---

6.4.1 Description

This resource represents the entities known to an NGSI-LD system.

6.4.2 Resource definition

Resource URI: - /entities/

6.4.3 Resource methods

6.4.3.1 POST
This method is bound to the operation "Create Entity" and shall exhibit the behaviour defined by clause 5.6.1, taking the
entity to be created from the HTTP request payload body. Figure 6.4.3.1-1 shows the Create Entity interaction and
Table 6.4.3.1-1 describes the request body and possible responses.

NGSI-LD Client NGSI-LD System

POST /entities

Entity

201 Created + Location Header

| | | Possible | |
| --- | --- | --- | --- |
| | 207 Multi-Status + Location Header | non-error | |
| | | responses | |

BatchOperationResult

Figure 6.4.3.1-1: Create Entity interaction


Table 6.4.3.1-1: Create Entity request body and possible responses

Request Body

Data Type Cardinality Remarks Entity 1 Payload body in the request contains a JSON-LD object which represents the entity that is to be created.

Remarks

Data Type Cardinality Response Codes

N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the relative path of the created entity.

BatchOperationR esult

1 207 Multi Status

The HTTP response shall include a "Location" HTTP header that contains the relative path of the created entity. If the entity input data matches to a registration, the relevant parts of the request are forwarded as a distributed operation. In the case when an error response is received back from any distributed operation, a response body containing the result returned from each registration is returned in a BatchOperationResult structure. Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

Response Body

| | ProblemDetails | 1 400 Bad | It is used to indicate that the request or its content | |
| --- | --- | --- | --- | --- |
| | (see IETF | Request | is incorrect, see clause 6.3.2. | |
| | RFC 7807 [10]) | | In the returned ProblemDetails structure, the detail | |

member should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 409 Conflict It is used to indicate that the entity or an exclusive or redirect registration defining the entity already exists, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

| | ProblemDetails | 1 422 | It is used to indicate that the operation is not | |
| --- | --- | --- | --- | --- |
| | (see IETF | Unprocessab | available, see clause 6.3.2. | |
| | RFC 7807 [10]) | le Entity | In the returned ProblemDetails structure, the detail | |

attribute should convey more information about the error.

6.4.3.2 GET This method is associated to the operation "Query Entities" and shall exhibit the behaviour defined by clause 5.7.2, providing entities as part of the HTTP response payload body. In addition to this method, an alternative way to perform "Query Entities" operations via POST is defined in clause 6.23. Figure 6.4.3.2-1 shows the query entities interaction.


Figure 6.4.3.2-1: Query Entities interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.4.3.2-1, the request headers that shall be supported by implementations are those defined in Table 6.4.3.2-2 and Table 6.4.3.2-3 describes the request body and possible responses.

Table 6.4.3.2-1: Query Entities URL parameters Name Data Type Cardinality Remarks attrs Comma separated list of strings

0..1 At least one among: type, attrs, q, or georel shall be present, unless the execution of the request is limited to local scope (see clause 5.5.13).

A synonym for a combination of the pick and q members. Deprecated

Each String is an Attribute (Property or Relationship) name. List of Attributes to be matched by the Entities and also included in the response, i.e. only Entities that contain at least one of the Attributes in attrs are to be included in the response, and only the Attributes listed in attrs are to be included in each of the Entities of the response. collation String 0..1 An ICU collation (see IETF RFC 6067 [36]), When defined, the Entities returned in the payload shall be ordered according to the collation given. It is part of Entity ordering.

containedBy Comma separated list of URIs

0..1 List of entity ids which have previously been encountered whilst retrieving the Entity Graph. Only applicable if joinLevel is present.

coordinates String 0..1 It shall be one if geometry or georel are present.

Coordinates serialized as a string as per clause 4.10. It is part of geoquery.


Name Data Type Cardinality Remarks csf String 0..1 Context Source filter as per clause 4.9.

datasetId Comma separated list of strings

0..1 Shall be valid URIs, "@none" for including
the default Attribute instances. Specifies
the datasetIds of the Attribute instances to
be selected for each matched Attribute as
per clause 4.5.5.
entityMap Boolean 0..1 If true, the location of the EntityMap used in
the operation is returned in the response.
entityMapLifetime String 0..1 Suggested duration, represented in
ISO 8601 [17] format [17], for which the
requester wants the EntityMap to exist. The
actual expiresAt time of the EntityMap shall
be set by the Context Broker or Context
Source, possibly overriding the requested
duration. Only applicable if entityMap is set
to true.

expandValues Comma separated list of strings

0..1 Each String is an Attribute (Property or Relationship) name. List of Attributes whose values shall be expanded into URIs according to the supplied @context prior to executing a query. It is part of query.

geometry String 0..1 It shall be 1 if georel or coordinates are present. At least one among: type, attrs, q, or geometry shall be present, unless the execution of the request is limited to local scope (see clause 5.5.13).

Geometry as per clause 4.10. It is part of geoquery.

geometryProperty String 0..1 It represents a Property name. In the case of GeoJSON Entity representation, this parameter indicates which GeoProperty to use for the top-level geometry field.

geoproperty String 0..1 It shall be ignored unless a geoquery is present.

It represents the name of the Property that contains the geospatial data that will be used to resolve the geoquery. By default, will be location (see clause 4.7).

georel String 0..1 It shall be 1 if geometry or coordinates are present.

Geo relationship as per clause 4.10. It is part of geoquery.

id Comma separated list of strings

0..1 Each String shall be a valid URI. List of entity ids to be retrieved.

idPattern Regular expression as defined by [11]

0..1 Regular expression that shall be matched
by entity ids.
join String 0..1 The type of Linked Entity retrieval to
apply (see clause 4.5.23). Allowed values:
"flat", "inline", "@none".
joinLevel Positive Integer 0..1 Depth of Linked Entity retrieval to
apply.
Only applicable if join parameter is present.

jsonKeys Comma separated list of strings

0..1 Each String is an Attribute (Property or Relationship) name. Values of the identified attributes are to be considered uninterpretable as JSON-LD and should not be expanded against the supplied @context using JSON-LD type coercion prior to executing the query.


Name Data Type Cardinality Remarks lang String 0..1 It represents the preferred natural language of the response. It is used to reduce languageMaps to a string or string array property in a single preferred language.

omit Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or a projected Attribute name). When defined, the listed Entity members are removed from each Entity within the payload.

orderBy Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or an Attribute name) appended with an optional sorting style ("asc", "desc", "dist-asc", "dist-desc") as per clause 4.23. When defined, the Entities returned in the payload shall be ordered according to members defined. It is part of Entity ordering.

orderFrom String 0..1 It shall be one if orderBy uses order by distance

Coordinates of a Geometry serialized as a string as per clause 4.10. It is part of Entity ordering. orderGeometry String 0..1 A Geometry type (with the exception of GeometryCollection) as defined by the GeoJSON specification (IETF RFC 7946 [8]). It is part of Entity ordering.

pick Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or a projected Attribute name). When defined, every Entity within the payload body is reduced down to only contain the listed Entity members.

q String 0..1 At least one among: type, attrs, q, or georel shall be present, unless the execution of the request is limited to local scope (see clause 5.5.13).

Query as per clause 4.9.

scopeQ String 0..1 Scope query (see clause 4.19).
splitEntities Boolean 0..1 If true it is assumed that single Entities are
distributed between different Context
Brokers and/or Context Sources and this
has to be taken into account when applying
any kind of filters (q, geoQ, scopeQ,
Attributes etc.). If false it is expected that
Context Broker and/or Context Source
always have complete Entities, which
allows applying filters locally.
The parameter does not apply in case
local is true.
The default value should be decided by
implementation; it should be configurable.

type String 0..1 At least one among: type, attrs, q, or georel shall be present, unless the execution of the request is limited to local scope (see clause 5.5.13).

Selection of Entity Types as per clause 4.17. "*" is also allowed as a value and local is implicitly set to true and shall not be explicitly set to false.

Table 6.4.3.2-2: Query Entities request Headers Name Data Type Cardinality Remarks NGSILD-EntityMap URI 0..1 If present, the EntityMap supplied is used for determining the set of Entities requested during the query operation. The location of the EntityMap used in the query operation is returned in the response.

Table 6.4.3.2-3: Query Entities request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

| | Data Type Cardinality | | Response Codes | | Remarks | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | Entity[] 1 | | 200 OK | | A response body containing the query | | | |
| | | | 201 Created (in | | result as a list of entities, unless the | | | |
| | | | case an EntityMap | | Accept Header indicates that the Entities | | | |
| | | | has been | | are to be rendered as GeoJSON. | | | |

(re)created)

A response body containing the query result as a list of entities, unless the Accept Header indicates that the Entities are to be rendered as GeoJSON. If an EntityMap has been requested, the HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource used in the operation.

Entity[] 1 203 Non Authoritative Information

As above, but returning an altered response body, amended to conform to a specific version of the NGSI-LD specification as mandated in clause 4.3.6.8. The response shall also include a "Preference-Applied" HTTP header set to "ngsi ld= ".

Response Body

GeoJSON FeatureCollection

1 200 OK 201 Created (in case an EntityMap has been (re)created)

If the Accept Header indicates that the Entities are to be rendered as GeoJSON, a response body containing the query result as GeoJSON FeatureCollection is returned. If an EntityMap has been requested, the HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource used in the operation.

1

ProblemDetails (see IETF RFC 7807 [10])

400 Bad Request

It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

| | ProblemDetails (see 1 | | 501 Not | | It is used by Registered Context | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | IETF RFC 7807 [10]) | | Implemented | | Sources to indicate that the data format | | | |
| | | | | | of the request is unsupported see clause | | | |

6.3.7.

6.4.3.3 DELETE

This method is associated to the operation "Purge Entities" and shall exhibit the behaviour defined by clause 5.6.21, providing entities as part of the HTTP response payload body. Figure 6.4.3.3-1 shows the query entities interaction.


Figure 6.4.3.3-1: Purge Entities interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.4.3.3-1, the request headers that shall be supported by implementations are those defined in Table 6.4.3.3-2 and Table 6.4.3.3-3 describes the request body and possible responses.

Table 6.4.3.3-1: Purge Entities URL parameters Name Data Type Cardinality Remarks coordinates String 0..1 It shall be one if geometry or georel are present.

Coordinates serialized as a string as per clause 4.10. It is part of geoquery.

csf String 0..1 Context Source filter as per clause 4.9.

drop Comma separated list of strings

0..1 Each String is an Attribute name. When defined, every Entity within the payload body is reduced to not contain the listed Entity members.

geometry String 0..1 It shall be 1 if georel or coordinates are present. At least one among: type, attrs, q, or geometry shall be present, unless the execution of the request is limited to local scope (see clause 5.5.13).

Geometry as per clause 4.10. It is part of geoquery.

geometryProperty String 0..1 It represents a Property name. In the case of GeoJSON Entity representation, this parameter indicates which GeoProperty to use for the top-level geometry field.

geoproperty String 0..1 It shall be ignored unless a geoquery is present.

It represents the name of the Property that contains the geospatial data that will be used to resolve the geoquery. By default, will be location (see clause 4.7).

georel String 0..1 It shall be 1 if geometry or coordinates are present.

Geo relationship as per clause 4.10. It is part of geoquery.

id Comma separated list of strings

0..1 Each String shall be a valid URI. List of entity ids to be retrieved.

idPattern Regular expression as defined by [11]

0..1 Regular expression that shall be matched by entity ids.

keep Comma separated list of strings

0..1 Each String is an Attribute name. When defined, every Entity within the payload body is reduced down to only contain the listed Entity members.

Query as per clause 4.9.

Name Data Type Cardinality Remarks q String 0..1 At least one among: type, attrs, q, or georel shall be present, unless the execution of the request is limited to local scope (see clause 5.5.13).

Selection of Entity Types as per clause 4.17. "*" is also allowed as a value and local is implicitly set to true and shall not be explicitly set to false.

scopeQ String 0..1 Scope query (see clause 4.19).
type String 0..1
At least one among: type,
attrs, q, or georel shall be
present, unless the
execution of the request is
limited to local scope (see
clause 5.5.13).

Table 6.4.3.3-2: Purge Entities request Headers Name Data Type Cardinality Remarks NGSILD-EntityMap URI 0..1 If present, the EntityMap supplied is used for determining the set of Entities requested during the purge operation. The location of the EntityMap used in the purge operation is returned in the response.

Table 6.4.3.3-3: Purge Entities request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Remarks

Data Type Cardinality Response Codes

| | N/A | | N/A | | | 204 No | | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | Content | | | | | |
| | BatchOperation | | 1 | | | 207 Multi | | | If the entity input data matches to a | | |
| | Result | | | | | Status | | | registration, the relevant parts of the request | | |

If the entity input data matches to a registration, the relevant parts of the request are forwarded as a distributed operation. In the case when an error response is received back from any distributed operation, a response body containing the result returned from each registration is returned in a BatchOperationResult structure. Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

Response Body

1 400 Bad Request

ProblemDetails (see IETF RFC 7807 [10])

It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Create Entity](/api-operations/provision/create-entity.md)
* [Operation: Purge Entities](/api-operations/provision/purge-entities.md)
* [Operation: Query Entities](/api-operations/consumption/query-entities.md)
* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.23](/framework/languages/entity-ordering.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.23](/framework/data-representation/ngsi-ld-linked-entity-retrieval.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
* [Clause 4.7](/framework/geo-temporal/geospatial-properties.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entities/
