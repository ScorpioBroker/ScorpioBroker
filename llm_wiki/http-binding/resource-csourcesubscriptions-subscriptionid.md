---
type: NGSI-LD Resource
title: "Resource: csourceSubscriptions/{subscriptionId}"
description: "6.13.1 Description This resource represents the context source registration subscription, identified by subscriptionId, known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.13
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.13"
---

6.13.1 Description

This resource represents the context source registration subscription, identified by subscriptionId, known to an NGSI-LD system.

6.13.2 Resource definition

Resource URI:

- /csourceSubscriptions/{subscriptionId} Resource URI variables for this resource are defined in Table 6.13.2-1.

Table 6.13.2-1: URI variables Name Definition subscriptionId Id (URI) of the concerned context source registration subscription.

6.13.3 Resource methods

6.13.3.1 GET
This method is associated to the operation "Retrieve Context Source Registration Subscription" and shall exhibit the
behaviour defined by clause 5.11.4. The subscription identifier is the value of the resource URI variable
"subscriptionId". Figure 6.13.3.1-1 shows the Retrieve Context Source Registration interaction and Table 6.13.3.1-1
describes the request body and possible responses.


Figure 6.13.3.1-1: Retrieve Context Source Registration Subscription interaction

Table 6.13.3.1-1: Retrieve Context Source Registration Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks Subscription 1 200 OK A response body containing the JSON-LD representation of the target context source registration subscription.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a subscription identifier (URI) not known to the system, see clause 6.3.2.

6.13.3.2 PATCH
This method is associated to the operation "Update Context Source Registration Subscription" and shall exhibit the
behaviour defined by clause 5.11.3. The subscription identifier is the value of the resource URI variable
"subscriptionId". Figure 6.13.3.2-1 shows the Update Context Source Registration Subscription interaction and
Table 6.13.3.2-1 describes the request body and possible responses.

Figure 6.13.3.2-1: Update Context Source Registration Subscription interaction

Table 6.13.3.2-1: Update Context Source Registration Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks Subscription Fragment 1 Subscription Fragment including id, type and any other context source registration subscription field to be changed.

| | | Data Type | | | Cardinality | | | Response Codes | | Remarks | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | N/A | | | N/A | | | 204 No Content | | | |
| | | ProblemDetails (see | | | 1 | | | 400 Bad Request | | It is used to indicate that the request | |
| | | IETF RFC 7807 [10]) | | | | | | | | or its content is incorrect, see | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a subscription identifier (URI) not known to the system, see clause 6.3.2.

6.13.3.3 DELETE

This method is associated to the operation "Delete Context Source Registration Subscription" and shall exhibit the behaviour defined by clause 5.11.6. The subscription identifier is the value of the resource URI variable "subscriptionId". Figure 6.13.3.3-1 shows the Delete Context Source Registration Subscription interaction and Table 6.13.3.3-1 describes the request body and possible responses.

Figure 6.13.3.3-1: Delete Context Source Registration Subscription interaction

Table 6.13.3.3-1: Delete Context Source Registration Subscription request body and possible responses

| | | Data Type | | | Cardinality | | | | | Remarks | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | | | |
| | N/A | | | | N/A | | | | | | |
| | | Data Type | | | Cardinality | | Response Codes | | | Remarks | |
| | N/A | | | | N/A | | 204 No Content | | | | |
| | ProblemDetails (see | | | | 1 | | 400 Bad Request | | It is used to indicate that the request or its | | |
| | IETF RFC 7807 [10]) | | | | | | | | content is incorrect, see clause 6.3.2. | | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a subscription identifier (URI) not known to the system, see clause 6.3.2.

# Related

* [Operation: Update Context Source Registration Subscription](/api-operations/csource-subscription/update-context-source-registration-subscription.md)
* [Operation: Retrieve Context Source Registration Subscription](/api-operations/csource-subscription/retrieve-context-source-registration-subscription.md)
* [Operation: Delete Context Source Registration Subscription](/api-operations/csource-subscription/delete-context-source-registration-subscription.md)
* [Clause 5.11.3](/api-operations/csource-subscription/update-context-source-registration-subscription.md)
* [Clause 5.11.4](/api-operations/csource-subscription/retrieve-context-source-registration-subscription.md)
* [Clause 5.11.6](/api-operations/csource-subscription/delete-context-source-registration-subscription.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: csourceSubscriptions/{subscriptionId}
