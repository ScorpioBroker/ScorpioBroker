---
type: NGSI-LD Resource
title: "Resource: jsonldContexts/"
description: "6.29.1 Description This resource represents the @contexts known to an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.29
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.29"
---

6.29.1 Description

This resource represents the @contexts known to an NGSI-LD system.

6.29.2 Resource definition

Resource URI: - /jsonldContexts/

6.29.3 Resource methods

6.29.3.1 POST
This method is bound to the operation "Add @context" and shall exhibit the behaviour defined by clause 5.13.2, taking
the @context to be added from the HTTP request payload body. Figure 6.29.3.1-1 shows the Add @context interaction
and Table 6.29.3.1-1 describes the request body and possible responses.


Figure 6.29.3.1-1: Add @context interaction

Table 6.29.3.1-1: Add @context request body and possible responses

Request Body

Data Type Cardinality Remarks JSON Object 1 Payload body in the request contains a JSON object that has a root node named @context, which represents a JSON-LD "local context".

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the local relative path of the added @context.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.29.3.2 GET
This method is associated to the operation "List @contexts" and shall exhibit the behaviour defined by clause 5.13.3,
and it provides information about stored @contexts as part of the HTTP response payload body. Figure 6.29.3.2-1
shows the List @contexts interaction.

Figure 6.29.3.2-1: List @contexts interaction

The request parameters that shall be supported by implementations are those defined in Table 6.29.3.2-1 and Table 6.29.3.2-2 describes the request body and possible responses.

| | Name | | | Data Type | | Cardinality | | | | | Remarks | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | details | | | Boolean | | 0..1 | | | Whether a list of URLs or a more detailed list of | | | | |
| | | | | | | | | | JSON Objects is requested. | | | | |
| | kind | | | String | | 0..1 | | | Can be either "Cached", "Hosted", or | | | | |
| | | | | | | | | | "ImplicitlyCreated". | | | | |

Table 6.29.3.2-1: List @contexts URL parameters

Table 6.29.3.2-2: List @contexts request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Data Type Cardinality Response Codes Remarks String[] or JSON Object[]

1 200 OK A response body containing a list of URLs or a list of JSON Objects, as defined in clause 5.13.3.5, representing metadata about stored @contexts.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Add @context](/api-operations/contexts/add-at-context.md)
* [Operation: List @contexts](/api-operations/contexts/list-at-contexts.md)
* [Clause 5.13.2](/api-operations/contexts/add-at-context.md)
* [Clause 5.13.3](/api-operations/contexts/list-at-contexts.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.29](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: jsonldContexts/
