---
type: NGSI-LD Resource
title: "Resource: csourceRegistrations/{registrationId}"
description: "6.9.1 Description This resource represents the context source registration, identified by registrationId, known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.9
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.9"
---

6.9.1 Description

This resource represents the context source registration, identified by registrationId, known to an NGSI-LD system.

6.9.2 Resource definition

Resource URI:

- /csourceRegistrations/{registrationId} Resource URI variables for this resource are defined in Table 6.9.2-1.

Table 6.9.2-1: URI variables Name Definition registrationId Id (URI) of the context source registration


6.9.3 Resource methods

6.9.3.1 GET
This method is associated with the operation "Retrieve Context Source Registration" and shall exhibit the behaviour
defined by clause 5.10.1. The registration identifier is the value of the resource URI variable "registrationId".
Figure 6.9.3.1-1 shows the Retrieve Context Source Registration interaction and Table 6.9.3.1-1 describes the request
body and possible responses.

Figure 6.9.3.1-1: Retrieve Context Source Registration interaction

Table 6.9.3.1-1: Retrieve Context Source Registration request body and possible responses Request Body Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks CSourceRegistration 1 200 OK A response body containing the JSON-LD representation of the target context source registration.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a context source registration identifier (URI) not known to the system, see clause 6.3.2.

6.9.3.2 PATCH

This method is bound to the "Update Context Source Registration" operation and shall exhibit the behaviour defined by clause 5.9.3. The context source registration identifier is the value of the resource URI variable "registrationId". The context source registration to be updated shall be contained in the HTTP request payload body. Figure 6.9.3.2-1 shows the Update Context Source Registration interaction and Table 6.9.3.2-1 describes the request body and possible responses.


Figure 6.9.3.2-1: Update Context Source Registration interaction

Table 6.9.3.2-1: Update Context Source Registration request body and possible responses

Request Body

Data Type Cardinality Remarks CSourceRegistration Fragment

1 Payload body in the request contains a JSON-LD object which represents the context source registration that is to be updated.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No Content The context source registration was successfully updated.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a context source registration identifier not known to the system, see clause 6.3.2.

6.9.3.3 DELETE

This method is associated to the operation "Delete Context Source Registration" and shall exhibit the behaviour defined by clause 5.9.4. The context source registration identifier is the value of the resource URI variable "registrationId". Figure 6.9.3.3-1 shows the Delete Context Source Registration interaction and Table 6.9.3.3-1 describes the request body and possible responses.

Figure 6.9.3.3-1: Delete Context Source Registration interaction

Table 6.9.3.3-1: Delete Context Source Registration request body and possible responses

| | | Data Type | | Cardinality | | | Remarks | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | |
| | | N/A | | N/A | | | | | |
| | | Data Type | | Cardinality | Response Codes | | Remarks | | |
| | | N/A | | N/A | 204 No Content | | | | |
| | | ProblemDetails (see | | 1 | 400 Bad Request | It is used to indicate that the request | | | |
| | | IETF RFC 7807 [10]) | | | | or its content is incorrect, see | | | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a context source registration identifier (URI) not known to the system, see clause 6.3.2.

# Related

* [Operation: Update Context Source Registration](/api-operations/registration/update-context-source-registration.md)
* [Operation: Delete Context Source Registration](/api-operations/registration/delete-context-source-registration.md)
* [Operation: Retrieve Context Source Registration](/api-operations/discovery/retrieve-context-source-registration.md)
* [Clause 5.10.1](/api-operations/discovery/retrieve-context-source-registration.md)
* [Clause 5.9.3](/api-operations/registration/update-context-source-registration.md)
* [Clause 5.9.4](/api-operations/registration/delete-context-source-registration.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.9](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: csourceRegistrations/{registrationId}
