---
type: NGSI-LD Resource
title: "Resource: entityOperations/query"
description: "6.23.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable querying for entities by means of a POST method."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.23
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.23"
---

6.23.1 Description

A sub-resource, pertaining to the entityOperations/ resource, intended to enable querying for entities by means of a
POST method. The behaviour of this clause mirrors the one in clause 6.4.3.2, which performs the "Query Entity"
operation (defined by clause 5.7.2) by means of a GET method. The reason to provide an alternative via POST is that,
using GET:
a) The client may end up assembling very long URLs, due to the URI parameters for id, q‚ type, attrs, etc., being
included in the URL. Problems with too long URLs may arise with some applications that cut URLs to a
maximum length.
b) There is a need to URL-encode the resulting URL. By using POST, there is no need to url-encode.

6.23.2 Resource definition

Resource URI: - /entityOperations/query


6.23.3 Resource methods

6.23.3.1 POST
This method is associated to the operation "Query Entities" and shall exhibit the behaviour defined by clause 5.7.2.
Figure 6.23.3.1-1 shows the operation interaction and Table 6.23.3.1-1 describes the request body and possible
responses.

Figure 6.23.3.1-1: Query Entity via POST interaction


Table 6.23.3.1-1: Query Entity via POST request body and possible responses

Request Body

Data Type Cardinality Remarks Query 1 Payload body in the request contains a JSON-LD object which represents the query to be performed.

Response Body

Data Type Cardinality Response Codes Remarks Entity[] 1 200 OK 201 Created (in case an EntityMap has been (re)created)

A response body containing the query result as a list of Entities.

The HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource created in the operation.

GeoJSON FeatureCollection

1 200 OK 201 Created (in case an EntityMap has been (re)created)

If the Accept Header indicates that the Entities are to be rendered as GeoJSON, a response body containing the query result as GeoJSON FeatureCollection is returned. The HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource created in the operation.

Entity[] 1 203 Non Authoritative Information

As above, but returning an altered response body, amended to conform to a specific version of the NGSI-LD specification as mandated in clause 4.3.6.8. The response shall also include a "Preference-Applied" HTTP header set to "ngsi ld= ".

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Clause 4.3.6.8](/framework/architecture/backwards-compatibility-of-context-source-payloads.md)
* [Clause 5.7.2](/api-operations/consumption/query-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.23](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entityOperations/query
