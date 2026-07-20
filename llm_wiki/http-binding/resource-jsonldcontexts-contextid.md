---
type: NGSI-LD Resource
title: "Resource: jsonldContexts/{contextId}"
description: "6.30.1 Description This resource represents a JSON-LD @context stored in the broker's internal @context storage."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.30
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.30"
---

6.30.1 Description

This resource represents a JSON-LD @context stored in the broker's internal @context storage.

6.30.2 Resource definition

Resource URI:

- /jsonldContexts/{contextId} Resource URI variables for this resource are defined in Table 6.30.2-1.

Table 6.30.2-1: URI variables Name Definition contextId Local identifier of the @context to be managed (served or deleted). For @contexts of kind "Cached" this can also be the original URL the broker downloaded the @context from.

6.30.3 Resource methods

6.30.3.1 GET
This method is associated to the operation "Serve @context" and shall exhibit the behaviour defined by clause 5.13.4.
The @context identifier is the value of the resource URI variable "contextId". Figure 6.30.3.1-1 shows the HTTP Serve
@context interaction.


Figure 6.30.3.1-1: Serve @context interaction

The request parameters that shall be supported by implementations are those defined in Table 6.30.3.1-1 and Table 6.30.3.1-2 describes the request body and possible responses.

Table 6.30.3.1-1: Serve @contexts URL parameters Name Data Type Cardinality Remarks details Boolean 0..1 Whether the content of the @context or its metadata is requested.

Table 6.30.3.1-2: Serve @context request body and possible responses

| | | Data Type | | Cardinality | | | | Remarks | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | |
| | | N/A | | N/A | | | | | |
| | | Data Type | | Cardinality | | Response Codes | | Remarks | |
| | | JSON Object | | 1 | | 200 OK | | If the parameter details are false or | |

Data Type Cardinality Response Codes Remarks JSON Object 1 200 OK If the parameter details are false or missing, response body contains a JSON object that has a root node named @context, which represents a JSON-LD "local context". If the parameter details are true, response body contains a JSON object as defined in clause 5.13.4.5, which metadata of a JSON-LD "local context".

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an @context identifier not known to the system, see clause 6.3.2.

ProblemDetails (see IETF RFC 7807 [10])

1 422 Unprocessable It is used when a client indicated an @context of type "Cached", see clause 6.3.2.

6.30.3.2 DELETE

This method is associated to the operation "Delete and Reload @context" and shall exhibit the behaviour defined by clause 5.13.5. The Entity identifier is the value of the resource URI variable "contextId". Figure 6.30.3.2-1 shows the delete entity interaction. The request parameters that shall be supported are those defined in Table 6.30.3.2-1 and Table 6.30.3.2-2 describes the request body and possible responses.


Table 6.30.3.2-1: Delete and Reload @context URL parameters Name Data Type Cardinality Remarks reload Boolean 0..1 indicates to perform a download and replace of the @context, as specified in clause 5.13.5.4.

Figure 6.30.3.2-1: Delete and Reload @context interaction

Table 6.30.3.2-2: Delete and Reload @context request body and possible responses

| | | | | Data Type | | | | Cardinality | | | Remarks | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | | | | | | |
| | | | N/A | | | | N/A | | | | | | | |
| | | | | Data Type | | | | Cardinality | | Response Codes | | Remarks | | |
| | | | N/A | | | | N/A | | | 204 No Content | | | | |
| | | | ProblemDetails (see | | | | 1 | | | 400 Bad Request | It is used to indicate that the request or | | | |
| | | | IETF RFC 7807 [10]) | | | | | | | | its content is incorrect, see clause 6.3.2. | | | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an @context identifier not known to the system, see clause 6.3.2.

It is used when re-downloading fails.

ProblemDetails (see IETF RFC 7807 [10])

1 504 Gateway Timeout

# Related

* [Operation: Serve @context](/api-operations/contexts/serve-at-context.md)
* [Operation: Delete and Reload @context](/api-operations/contexts/delete-and-reload-at-context.md)
* [Clause 5.13.4](/api-operations/contexts/serve-at-context.md)
* [Clause 5.13.5](/api-operations/contexts/delete-and-reload-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.30](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: jsonldContexts/{contextId}
