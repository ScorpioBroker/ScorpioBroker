---
type: NGSI-LD Resource
title: "Resource: csourceRegistrations/"
description: "6.8.1 Description This resource represents the context source registrations known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.8
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.8"
---

6.8.1 Description

This resource represents the context source registrations known to an NGSI-LD system.

6.8.2 Resource definition

Resource URI: - /csourceRegistrations/

6.8.3 Resource methods

6.8.3.1 POST This method is bound to the operation "Register Context Source" and shall exhibit the behaviour defined by clause 5.9.2, taking the context source registration to be created from the HTTP request payload body. Figure 6.8.3.1-1 shows the Register Context Source interaction and Table 6.8.3.1-1 describes the request body and possible responses.

Figure 6.8.3.1-1: Register Context Source interaction


Table 6.8.3.1-1: Register Context Source request body and possible responses

Request Body

Data Type Cardinality Remarks CSourceRegistration 1 Payload body in the request contains a JSON-LD object which represents the context source registration that is to be created.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the relative path of the created context source registration.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 409 Conflict It is used to indicate that the context source registration already exists, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 422 Unprocessable Context Source Registration

It is used to indicate that the operation is not available see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.8.3.2 GET
This method is associated to the operation "Query Context Source Registrations" and shall exhibit the behaviour defined
by clause 5.10.2, i.e. the parameters in the request describe entity related information, but instead of directly providing
this entity information, the context source registration data, which describes context sources that can possibly provide
the information, are returned as part of the HTTP response payload body. Figure 6.8.3.2-1 shows the Query Context
Source Registrations interaction.

Figure 6.8.3.2-1: Query Context Source Registrations interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.8.3.2-1 and Table 6.8.3.2-2 describes the request body and possible responses.

Table 6.8.3.2-1: Query Context Source Registrations URL parameters Name Data Type Cardinality Remarks attrs Comma separated list strings

A synonym for a combination of the pick and q members. Deprecated

0..1
At least one among: type,
attrs, q, or georel shall be
present.

Each String is an Attribute (Property or Relationship) name. List of Attributes (Properties or Relationships) to be retrieved.

Coordinates serialized as a string as per clause 4.10. It is part of geoquery.

coordinates String 0..1 It shall be one if geometry or georel are present.

csf String 0..1 Context Source filter as per clause
4.9.
endTimeAt String 0..1 It represents the endTimeAt parameter as
defined by clause 4.1.
It shall be a DateTime. Cardinality shall
be 1 if timerel is equal to "between".

Geometry as per clause 4.10. It is part of geoquery.

geometry String 0..1 It shall be 1 if georel or coordinates are present. At least one among: type, attrs, q, or georel shall be present.

geometryProperty String 0..1 It represents a Property name. In the case of GeoJSON Entity representation, this parameter indicates which GeoProperty to use for the top level geometry field.

| geoproperty | String | 0..1 | | It represents the name of the Property | | |
| --- | --- | --- | --- | --- | --- | --- |
| | | It shall be ignored if no | | that contains the geospatial data that will | | |
| | | geoquery is present. | | be used to resolve the geoquery. | | |
| georel | String | 0..1 | | Geo relationship as per clause 4.10. It is | | |

part of geoquery.

georel String 0..1 It shall be 1 if geometry or coordinates are present.

id Comma separated list of strings

0..1 Each String shall be a valid URI. List of entity ids to be retrieved.

idPattern Regular expression as defined by [11]

0..1 Regular expression that shall be matched by entity ids satisfying the query lang String 0..1 It represents the preferred natural language of the response. It is used to reduce languageMaps to a string or string array property in a single preferred language.

omit Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or a projected Attribute name).

pick Comma separated list of strings

0..1 Each String is an Entity member ("id", "type", "scope" or a projected Attribute name).

Query as per clause 4.9.

q String 0..1 At least one among: type, attrs, q, or georel shall be present.

scopeQ String 0..1 Scope query (see clause 4.19).
timeAt String 0..1 It represents the timeAt parameter as
defined by clause 4.1.
It shall be a DateTime. Cardinality shall
be 1 if timerel is present.


It represents a Temporal Property name.

Name Data Type Cardinality Remarks timeproperty String 0..1 It shall be ignored if no temporal query is present.

Allowed values: "observedAt", "createdAt", "modifiedAt" and "deletedAt". If not specified, the default is "observedAt". (See clause 4.8). timerel String 0..1 It represents the temporal relationship as defined by clause 4.1.

Allowed values: "before", "after", "between".

Selection of Entity Types as per clause 4.17.

type String 0..1 At least one among: type, attrs, q, or georel shall be present.

Table 6.8.3.2-2: Retrieve Context Source Registrations request body and possible responses

| | | Data Type | Cardinality | | | | Remarks | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | |
| | N/A | | N/A | | | | | | |
| | | Data Type | Cardinality | | Response Codes | | Remarks | | |
| | CSourceRegistration[] | | 1 | | 200 OK | | A response body containing the query | | |

result as an array of context source registrations.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Register Context Source](/api-operations/registration/register-context-source.md)
* [Operation: Query Context Source Registrations](/api-operations/discovery/query-context-source-registrations.md)
* [Clause 4.1](/framework/introduction.md)
* [Clause 4.10](/framework/languages/ngsi-ld-geoquery-language.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 4.19](/framework/languages/ngsi-ld-scope-query-language.md)
* [Clause 4.8](/framework/geo-temporal/temporal-properties.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.10.2](/api-operations/discovery/query-context-source-registrations.md)
* [Clause 5.9.2](/api-operations/registration/register-context-source.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.8](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: csourceRegistrations/
