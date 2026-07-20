---
type: NGSI-LD Resource
title: "Resource: csourceSubscriptions/"
description: "6.12.1 Description This resource represents the context source registration subscriptions known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.12
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.12"
---

6.12.1 Description

This resource represents the context source registration subscriptions known to an NGSI-LD system.

6.12.2 Resource definition

Resource URI: - /csourceSubscriptions/

6.12.3 Resource methods

6.12.3.1 POST
This method is bound to the operation "Create Context Source Registration Subscription" and shall exhibit the
behaviour defined by clause 5.11.2, taking the context source registration subscription to be created from the HTTP
request payload body. Figure 6.12.3.1-1 shows the Create Context Source Registration Subscription interaction and
Table 6.12.3.1-1 describes the request body and possible responses.


Figure 6.12.3.1-1: Create Context Source Registration Subscription interaction

Table 6.12.3.1-1: Create Context Source Registration Subscription request body and possible responses

Request Body

Data Type Cardinality Remarks Subscription 1 Payload body in the request contains a JSON-LD object which represents the context source registration subscription that is to be created.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the relative path of the created context source registration subscription.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 409 Conflict It is used to indicate that the context source registration subscription already exists, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.12.3.2 GET This method is associated to the operation "Query Context Source Registration Subscriptions" and shall exhibit the behaviour defined by clause 5.11.5, providing the context source registration subscription data as part of the HTTP response payload body. Figure 6.12.3.2-1 shows the Query Context Source Registration Subscriptions interaction.

Figure 6.12.3.2-1: Query Context Source Registration Subscriptions interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.12.3.2-1 and Table 6.12.3.2-2 describes the request body and possible responses.


Table 6.12.3.2-1: Query Context Source Registration Subscriptions URL parameters Name Data Type Cardinality Remarks limit Number 0..1 Maximum number of subscriptions to be retrieved

Table 6.12.3.2-2: Query Context Source Registration Subscriptions request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks Subscription[] 1 200 OK A response body containing a list of context source registration subscriptions.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Create Context Source Registration Subscription](/api-operations/csource-subscription/create-context-source-registration-subscription.md)
* [Operation: Query Context Source Registration Subscriptions](/api-operations/csource-subscription/query-context-source-registration-subscriptions.md)
* [Clause 5.11.2](/api-operations/csource-subscription/create-context-source-registration-subscription.md)
* [Clause 5.11.5](/api-operations/csource-subscription/query-context-source-registration-subscriptions.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: csourceSubscriptions/
