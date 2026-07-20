---
type: NGSI-LD Resource
title: "Resource: temporal/entityOperations/query"
description: "6.24.1 Description A sub-resource, pertaining to the temporal/entityOperations/ resource, intended to enable temporal querying for entities by means of a POST method."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.24
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.24"
---

6.24.1 Description

A sub-resource, pertaining to the temporal/entityOperations/ resource, intended to enable temporal querying for entities
by means of a POST method. The behaviour of this clause mirrors the one in clause 6.18.3.2, which performs the
"Query Temporal Evolution of Entities" (defined by clause 5.7.4) operation by means of a GET method.
The reason to provide an alternative via POST is that, using GET:
a) The client may end up assembling very long URLs, due to the URI parameters for id, q‚ type, attrs, etc., being
included in the URL. Problems with too long URLs may arise with some applications that cut URLs to a
maximum length.
b) There is a need to URL-encode the resulting URL. By using POST, there is no need to url-encode.

6.24.2 Resource definition

Resource URI:

- /temporal/entityOperations/query


6.24.3 Resource methods

6.24.3.1 POST
This method is associated to the operation "Query Temporal Evolution of Entities" and shall exhibit the behaviour
defined by clause 5.7.4. Figure 6.24.3.1-1 shows the operation interaction and Table 6.24.3.1-1 describes the request
body and possible responses.

Figure 6.24.3.1-1: Temporal Query Entity via POST interaction

Table 6.24.3.1-1: Temporal Query Entity via POST request body and possible responses

Request Body

Data Type Cardinality Remarks Query 1 Payload body in the request contains a JSON-LD object which represents the query to be performed.

Response Body

Data Type Cardinality Response Codes Remarks EntityTemporal[] 1 200 OK 201 Created (in case an EntityMap has been (re)created)

A response body containing the query result as a list of Entities.

The HTTP response shall include an "NGSILD-EntityMap" HTTP header that contains the resource URI of the EntityMap resource created in the operation.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Clause 5.7.4](/api-operations/consumption/query-temporal-evolution-of-entities.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.24](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entityOperations/query
