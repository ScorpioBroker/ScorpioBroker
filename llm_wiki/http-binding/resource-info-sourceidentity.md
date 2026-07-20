---
type: NGSI-LD Resource
title: "Resource: info/sourceIdentity"
description: "6.33.1 Description This resource represents identity information about the Context Source itself."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.33
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.33"
---

6.33.1 Description

This resource represents identity information about the Context Source itself.

6.33.2 Resource definition

Resource URI: - /info/sourceIdentity

6.33.3 Resource methods

6.33.3.1 GET This method is associated to the operation "Retrieve Context Source Identity Information" and shall exhibit the behaviour defined by clause 5.15.1. Figure 6.33.3.1-1 shows the Retrieve Context Source Identity Information interaction and Table 6.33.3.1-1 describes the request body and possible responses.

Figure 6.33.3.1-1: Retrieve Context Source Identity Information

NGSI-LD Client NGSI-LD System

GET /info/sourceIdentity

200 OK

ContextSourceIdentity


Table 6.33.3.1-1: Retrieve Context Source Identity Information request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks ContextSourceIdentity 1 200 No Content A response body containing the JSON-LD representation of the Context Source Identity Information.

ProblemDetails (see IETF RFC 7807 [10])

1 501 Not Implemented

It is used by Context Sources to indicate that retrieval of the Context Source information is unsupported see clause 5.15.1.4.

# Related

* [Operation: Retrieve Context Source Identity Information](/api-operations/source-identity/retrieve-context-source-identity-information.md)
* [Clause 5.15.1](/api-operations/source-identity/retrieve-context-source-identity-information.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.33](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: info/sourceIdentity
