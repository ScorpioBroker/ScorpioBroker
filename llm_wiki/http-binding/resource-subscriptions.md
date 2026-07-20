---
type: NGSI-LD Resource
title: "Resource: subscriptions/"
description: "6.10.1 Description This resource represents the subscriptions known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.10
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.10"
---

6.10.1 Description

This resource represents the subscriptions known to an NGSI-LD system.

6.10.2 Resource definition

Resource URI: - /subscriptions/

6.10.3 Resource methods

6.10.3.1 POST This method is bound to the operation "Create Subscription" and shall exhibit the behaviour defined by clause 5.8.1, taking the subscription to be created from the HTTP request payload body. Figure 6.10.3.1-1 shows the Create Subscription interaction and Table 6.10.3.1-1 describes the request body and possible responses.

Figure 6.10.3.1-1: Create Subscription interaction


Table 6.10.3.1-1: Create Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks Subscription 1 Payload body in the request contains a JSON-LD object which represents the subscription that is to be created.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the relative path of the created subscription.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 409 Conflict It is used to indicate that the subscription already exists see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.10.3.2 GET
This method is associated to the operation "Query Subscriptions" and shall exhibit the behaviour defined by
clause 5.8.4, providing the subscription data as part of the HTTP response payload body. Figure 6.10.3.2-1 shows the
Query Subscriptions interaction.

Figure 6.10.3.2-1: Query Subscriptions interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.10.3.2-1 and Table 6.10.3.2-2 describes the request body and possible responses.

Table 6.10.3.2-1: Query Subscriptions URL parameters Name Data Type Cardinality Remarks limit Number 0..1 Maximum number of subscriptions to be retrieved


Table 6.10.3.2-2: Query Subscriptions request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks Subscription[] 1 200 OK A response body containing a list of subscriptions.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Create Subscription](/api-operations/subscription/create-subscription.md)
* [Operation: Query Subscriptions](/api-operations/subscription/query-subscriptions.md)
* [Clause 5.8.1](/api-operations/subscription/create-subscription.md)
* [Clause 5.8.4](/api-operations/subscription/query-subscriptions.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.10](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: subscriptions/
