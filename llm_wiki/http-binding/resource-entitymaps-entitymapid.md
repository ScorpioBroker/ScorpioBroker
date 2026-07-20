---
type: NGSI-LD Resource
title: "Resource: entityMaps/{entityMapId}"
description: "6.32.1 Description This resource represents an EntityMap available in the broker's internal storage or memory."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.32
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.32"
---

6.32.1 Description

This resource represents an EntityMap available in the broker's internal storage or memory.

6.32.2 Resource definition

Resource URI:

- /entityMaps/{entityMapId} Resource URI variables for this resource are defined in Table 6.32.2-1.

Table 6.32.2-1: URI variables Name Definition entityMapId Id (URI) of the EntityMap to be retrieved, updated or deleted.

6.32.3 Resource methods

6.32.3.1 GET This method is associated to the operation "Retrieve EntityMap" and shall exhibit the behaviour defined by clause 5.14.1. The EntityMap identifier is the value of the resource URI variable "entityMapId". Figure 6.32.3.1-1 shows the Retrieve EntityMap interaction and Table 6.32.3.1-1 describes the request body and possible responses.

NGSI-LD Client NGSI-LD System

GET /entityMaps/{entityMapId}

200 OK

EntityMap

Figure 6.32.3.1-1: Retrieve EntityMap

Table 6.32.3.1-1: Retrieve EntityMap request body and possible responses

| | | | | Data Type | | Cardinality | | Remarks | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | |
| | | | | N/A | | N/A | | | |
| | | | | Data Type | | Cardinality | Response Codes | Remarks | |
| | | | | EntityMap | | 1 | 200 OK | A response body containing the JSON-LD | |

representation of the target entity.

ProblemDetails (see IETF RFC 7807 [10])

Response Body

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an EntityMap identifier not known to the system, see clause 6.3.2.


6.32.3.2 PATCH This method is associated to the operation "Update EntityMap" and shall exhibit the behaviour defined by clause 5.14.2. The EntityMap identifier is the value of the resource URI variable "entityMapId". Figure 6.32.3.2-1 shows the Update EntityMap interaction and Table 6.32.3.2-1 describes the request body and possible responses.

NGSI-LD Client NGSI-LD System

PATCH /entityMaps/{entityMapId}

EntityMap

204 No Content

Figure 6.32.3.2-1: Update EntityMap

Table 6.32.3.2-1: Update EntityMap request body and possible responses

Request Body

Data Type Cardinality Remarks EntityMap Fragment 1 Payload body in the request contains a JSON-LD object which represents the EntityMap fragment with which the EntityMap is to be updated.

| | N/A | N/A | 204 No Content | | | |
| --- | --- | --- | --- | --- | --- | --- |
| | ProblemDetails (see | 1 | 400 Bad Request | It is used to indicate that the request or | | |
| | IETF RFC 7807 [10]) | | | its content is incorrect, see clause 6.3.2. | | |

Data Type Cardinality Response Codes Remarks

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an EntityMap identifier not known to the system, see clause 6.3.2.

6.32.3.3 DELETE

This method is associated to the operation "Delete EntityMap" and shall exhibit the behaviour defined by clause 5.14.3. The Entity identifier is the value of the resource URI variable "contextId". Figure 6.32.3.3-1 shows the delete entity interaction and Table 6.32.3.3-1 describes the request body and possible responses.

NGSI-LD Client NGSI-LD System

DELETE /entityMaps/{entityMapId}

204 No Content

Figure 6.32.3.3-1: Delete and Reload @context interaction


Table 6.32.3.3-1: Delete EntityMap request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No Content ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an @context identifier not known to the system, see clause 6.3.2.

# Related

* [Operation: Retrieve EntityMap](/api-operations/entity-mapping/retrieve-entitymap.md)
* [Operation: Update EntityMap](/api-operations/entity-mapping/update-entitymap.md)
* [Operation: Delete EntityMap](/api-operations/entity-mapping/delete-entitymap.md)
* [Clause 5.14.1](/api-operations/entity-mapping/retrieve-entitymap.md)
* [Clause 5.14.2](/api-operations/entity-mapping/update-entitymap.md)
* [Clause 5.14.3](/api-operations/entity-mapping/delete-entitymap.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.32](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entityMaps/{entityMapId}
