---
type: NGSI-LD Resource
title: "Resource: temporal/entities/"
description: "6.18.1 Description This resource represents the Temporal Evolution of Entities known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.18
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.18"
---

6.18.1 Description

This resource represents the Temporal Evolution of Entities known to an NGSI-LD system.

6.18.2 Resource definition

Resource URI:

- /temporal/entities/


6.18.3 Resource methods

6.18.3.1 POST
This method is associated to the operation "Create or Update Temporal Evolution of an Entity" and shall exhibit the
behaviour defined by clause 5.6.11, taking the temporal representation of Entity to be created from the HTTP request
payload body. Figure 6.18.3.1-1 shows this interaction and Table 6.18.3.1-1 describes the request body and possible
responses.

Figure 6.18.3.1-1: Create or Update Temporal Evolution of an Entity interaction

Table 6.18.3.1-1: Create or Update Temporal Evolution of an Entity request body and possible responses

Request Body

Data Type Cardinality Remarks EntityTemporal 1 Payload body in the request contains a JSON-LD object which represents the temporal representation of the Entity that is to be created (or updated).

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created Upon creation success, the HTTP response shall include a "Location" HTTP header that contains the relative path of the created entity. N/A N/A 204 No Content Upon update success. ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail member should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 422 Unprocessable Entity

It is used to indicate that the operation is not available, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.18.3.2 GET This method is associated to the operation "Query Temporal Evolution of Entities" and shall exhibit the behaviour defined by clause 5.7.4, providing the Temporal Evolution of the matching Entities as part of the HTTP response payload body. In addition to this method, an alternative way to perform "Query Temporal Evolution of Entities" operations via POST is defined in clause 6.24. Figure 6.18.3.2-1 shows this interaction.


Figure 6.18.3.2-1: Query Temporal Evolution of Entities interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.18.3.2-1 and Table 6.18.3.2-2 describes the request body and possible responses.

Table 6.18.3.2-1: Query Temporal Evolution of Entities URL parameters Name Data Type Cardinality Remarks aggrMethods Comma separated list of strings

0..1
It shall be 1 if
aggregatedValues is
present in the options
parameter

Each String represents an aggregation method, as defined by clause 4.5.19. Only applicable if "aggregatedValues" is present in the format or options parameter. aggrPeriodDuration String 0..1 It represents the duration of each period used for the aggregation as defined by clause 4.5.19. If not specified, it defaults to a duration of 0 seconds and is interpreted as a duration spanning the whole time-range specified by the temporal query. Only applicable if "aggregatedValues" is present in the format or options parameter.

attrs Comma separated list of strings

0..1 It shall be 1 if type is not present, unless the execution of the request is limited to local scope (see clause 5.5.13).

A synonym for a combination of the pick and q parameters. Deprecated

Each String is an Attribute (Property or Relationship) name. List of Attributes (Properties or Relationships) to be retrieved. collation String 0..1 An ICU collation (see IETF RFC 6067 [36]), When defined, the Entities returned in the payload shall be ordered according to the collation given. It is part of Entity ordering.

coordinates String 0..1 It shall be one if georel or geometry are present

Coordinates serialized as a string as per clause 4.10. It is part of geoquery.

csf String 0..1 Context Source filter as per clause 4.9.

Name Data Type Cardinality Remarks datasetId Comma separated list of strings

0..1 Shall be valid URIs, "@none" for including
the default Attribute instances. Specifies
the datasetIds of the Attribute instances to
be selected for each matched Attribute as
per clause 4.5.5.
endTimeAt String 0..1 It is representing the endTimeAt parameter
as defined by clause 4.11.
It shall be a DateTime.
Cardinality shall be 1 if timerel is equal to
"between".
entityMap Boolean 0..1 If true, the location of the EntityMap used
in the operation is returned in the response.
entityMapLifetime String 0..1 Suggested duration, represented in
ISO 8601 [17] format, for which the
requester wants the EntityMap to exist. The
actual expiresAt time of the EntityMap shall
be set by the Context Broker or Context
Source, possibly overriding the requested
duration. Only applicable if entityMap is set
to true.

| entityMap | Boolean | 0..1 | | | If true, the location of the EntityMap used | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | in the operation is returned in the response. | | | | |
| entityMapLifetime | String | 0..1 | | | Suggested duration, represented in | | | | |

expandValues Comma separated list of strings

0..1 Each String is an Attribute (Property or Relationship) name. List of Attributes whose values shall be expanded into URIs according to the supplied @context prior to executing a query. It is part of query.

Geometry as per clause 4.10. It is part of geoquery.

geometry String 0..1 It shall be 1 if georel or coordinates are present geoproperty String 0..1 It shall be ignored if no geoquery is present

The name of the GeoProperty that contains the geospatial data that will be used to resolve the geoquery. By default, will be location. (See clause 4.7).

Geo relationship as per clause 4.10. It is part of geoquery.

georel String 0..1 It shall be 1 if geometry or coordinates are present id Comma separated list of strings

0..1 Each String shall be a valid URI. List of entity ids to be retrieved.

idPattern Regular expression as defined by [11]

0..1 Regular expression that shall be matched by entity ids.

jsonKeys Comma separated list of strings

0..1 Each String is an Attribute (Property or
Relationship) name.
Values of the identified attributes are to be
considered uninterpretable as JSON-LD
and should not be expanded against the
supplied @context using JSON-LD type
coercion prior to executing the query.
lang String 0..1 It represents the preferred natural language
of the response.
It is used to reduce languageMaps to a
string or string array property in a single
preferred language.
lastN Positive integer 0..1 Only the last n instances, per Attribute, per
Entity (under the specified time interval)
shall be retrieved.

omit Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or an Attribute name). When defined, the listed Entity members are removed from each Entity within the payload. orderBy String 0..1 The Entity member ("id") appended with an optional sorting style ("asc", "desc") as per clause 4.23. When defined, the Entities returned in the payload shall be ordered according to members defined. It is part of Entity ordering.


Name Data Type Cardinality Remarks pick Comma separated list of strings

0..1 Each String is an Entity member ("id",
"type", "scope" or an Attribute name).
When defined, every Entity within the
payload body is reduced down to only
contain the listed Entity members.
q String 0..1 Query as per clause 4.9.
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
timeAt String 1 representing the timeAt parameter as
defined by clause 4.11.
It shall be a DateTime.
timeproperty String 0..1 It represents a Temporal Property name.
Allowed values: "observedAt",
"createdAt", "modifiedAt" and
"deletedAt". If not specified, the
default is "observedAt". (See clause
4.8).
timerel String 1 It represents the temporal relationship as
defined by clause 4.11.
Allowed values: "before", "after",
"between".

type String 0..1 It shall be 1 if attrs is not present, unless the execution of the request is limited to local scope (see clause 5.5.13).

Selection of Entity Types as per clause 4.17. "*" is also allowed as a value and local is implicitly set to true and shall not be explicitly set to false.

Table 6.18.3.2-2: Query Temporal Evolution of Entities request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

| | Data Type | Cardinality | | Response Codes | | Remarks | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | EntityTemporal[] | 1 | | 200 OK | A response body containing the query | | | | |
| | | | | 201 Created (in | result as a list of temporal representation | | | | |

case an EntityMap has been (re)created)

A response body containing the query result as a list of temporal representation of Entities. If an EntityMap has been requested, the HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource used in the operation.

| | EntityTemporal[] | 1 | | 203 Non | As above, but returning an altered | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | Authoritative | response body, amended to conform to | | | | |
| Response Body | | | | Information | a specific version of the NGSI-LD | | | | |

As above, but returning an altered response body, amended to conform to a specific version of the NGSI-LD specification as mandated in clause 4.3.6.8. The response shall also include a "Preference-Applied" HTTP header set to "ngsi ld= ".

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Create or Update (Upsert) Temporal Evolution of an Entity](/api-operations/provision/create-or-update-upsert-temporal-evolution-of-an-entity.md)
* [Operation: Query Temporal Evolution of Entities](/api-operations/consumption/query-temporal-evolution-of-entities.md)
* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.11](/framework/languages/ngsi-ld-temporal-query-language.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.23](/framework/languages/entity-ordering.md)
* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 4.5.19](/framework/data-representation/aggregated-temporal-representation-of-an-entity.md)
* [Clause 4.5.5](/framework/data-representation/multi-attribute-support.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.18](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entities/
