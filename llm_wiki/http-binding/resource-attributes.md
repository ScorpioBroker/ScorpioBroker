---
type: NGSI-LD Resource
title: "Resource: attributes/"
description: "6.27.1 Description This resource represents the attributes available in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.27
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.27"
---

6.27.1 Description

This resource represents the attributes available in an NGSI-LD system.

6.27.2 Resource definition

Resource URI: - /attributes/

6.27.3 Resource methods

6.27.3.1 GET
This method is associated to the operations "Retrieve Available Attributes" and "Retrieve Details of Available
Attributes" (if the details parameter is set to true) and shall exhibit the behaviour defined by clauses 5.7.8 and 5.7.9
respectively.

Figure 6.27.3.1-1: Retrieve Available Attributes interaction

The request parameters that shall be supported are those defined in Table 6.27.3.1-1 and Table 6.27.3.1-2 describes the request body and possible responses.

Table 6.27.3.1-1: Retrieve Available Attributes URL parameters
Name Data Type Cardinality Remarks
details Boolean 0..1 If true, then detailed attribute information represented as an array with elements of
the Attribute data structure (clause 5.2.28) is to be returned.


Table 6.27.3.1-2: Retrieve Available Attributes request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks
AttributeList 1 200 OK A response body containing the JSON-LD
representation of the AttributeList
(clause 5.2.27) is to be returned, unless
details=true is specified.
Attribute[] 1 200 OK If details=true is specified, a response
body containing a JSON-LD array with
elements of the Attribute data structure
(clause 5.2.28) is to be returned.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Retrieve Available Attributes](/api-operations/consumption/retrieve-available-attributes.md)
* [Operation: Retrieve Details of Available Attributes](/api-operations/consumption/retrieve-details-of-available-attributes.md)
* [Clause 5.2.27](/api-operations/data-types/attributelist.md)
* [Clause 5.2.28](/api-operations/data-types/attribute.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.27](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: attributes/
