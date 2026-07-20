---
type: NGSI-LD Resource
title: "Resource: subscriptions/{subscriptionId}"
description: "6.11.1 Description This resource represents a subscription known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.11
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.11"
---

6.11.1 Description

This resource represents a subscription known to an NGSI-LD system.

6.11.2 Resource definition

Resource URI:

- /subscriptions/{subscriptionId} Resource URI variables for this resource are defined in Table 6.11.2-1.

Table 6.11.2-1: URI variables Name Definition subscriptionId Id (URI) of the concerned subscription

6.11.3 Resource methods

6.11.3.1 GET This method is associated to the operation "Retrieve Subscription" and shall exhibit the behaviour defined by clause 5.8.3. The subscription identifier is the value of the resource URI variable "subscriptionId". Figure 6.11.3.1-1 shows the Retrieve Subscription interaction and Table 6.11.3.1-1 describes the request body and possible responses.

Figure 6.11.3.1-1: Retrieve Subscription interaction

Table 6.11.3.1-1: Retrieve Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Data Type Cardinality Response Codes Remarks Subscription 1 200 OK A response body containing the JSON-LD representation of the target subscription.

ProblemDetails (see IETF RFC 7807 [10])

Response Body

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a subscription identifier (URI) not known to the system, see clause 6.3.2.

6.11.3.2 PATCH This method is associated to the operation "Update Subscription" and shall exhibit the behaviour defined by clause 5.8.2. The subscription identifier is the value of the resource URI variable "subscriptionId". Figure 6.11.3.2-1 shows the Update Subscription interaction and Table 6.11.3.2-1 describes the request body and possible responses.

Figure 6.11.3.2-1: Update Subscription interaction

Table 6.11.3.2-1: Update Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks Subscription Fragment 1 Subscription Fragment including id, type and any other subscription field to be changed

| | | | | | N/A | N/A | | 204 No Content | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | ProblemDetails (see | 1 | | 400 Bad Request | | It is used to indicate that the request | | |
| | | | | | IETF RFC 7807 [10]) | | | | | or its content is incorrect, see | | |

Data Type Cardinality Response Codes Remarks

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a subscription identifier (URI) not known to the system, see clause 6.3.2.

6.11.3.3 DELETE

This method is associated to the operation "Delete Subscription" and shall exhibit the behaviour defined by clause 5.8.5. The subscription identifier is the value of the resource URI variable "subscriptionId". Figure 6.11.3.3-1 shows the Delete Subscription interaction and Table 6.11.3.3-1 describes the request body and possible responses.


Figure 6.11.3.3-1: Delete Subscription interaction

Table 6.11.3.3-1: Delete Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No Content ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a subscription identifier (URI) not known to the system, see clause 6.3.2.

# Related

* [Operation: Update Subscription](/api-operations/subscription/update-subscription.md)
* [Operation: Retrieve Subscription](/api-operations/subscription/retrieve-subscription.md)
* [Operation: Delete Subscription](/api-operations/subscription/delete-subscription.md)
* [Clause 5.8.2](/api-operations/subscription/update-subscription.md)
* [Clause 5.8.3](/api-operations/subscription/retrieve-subscription.md)
* [Clause 5.8.5](/api-operations/subscription/delete-subscription.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.11](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: subscriptions/{subscriptionId}
