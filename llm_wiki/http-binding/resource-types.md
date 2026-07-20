---
type: NGSI-LD Resource
title: "Resource: types/"
description: "6.25.1 Description This resource represents the entity types available in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.25
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.25"
---

6.25.1 Description

This resource represents the entity types available in an NGSI-LD system.


6.25.2 Resource definition

Resource URI: - /types/

6.25.3 Resource methods

6.25.3.1 GET
This method is associated to the operations "Retrieve Available Entity Types" and "Retrieve Details of Available Entity
Types" (if the details parameter is set to true) and shall exhibit the behaviour defined by clauses 5.7.5 and 5.7.6
respectively.

Figure 6.25.3.1-1: Retrieve Available Entity Types interaction

The request parameters that shall be supported are those defined in Table 6.25.3.1-1 and Table 6.25.3.1-2 describes the request body and possible responses.

Table 6.25.3.1-1: Retrieve Available Entity Types URL parameters
Name Data Type Cardinality Remarks
details Boolean 0..1 If true, then detailed entity type information represented as an array with elements
of the Entity Type data structure (clause 5.2.25) is to be returned.

Table 6.25.3.1-2: Retrieve Available Entity Types request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks
EntityTypeList 1 200 OK A response body containing the JSON-LD
representation of the EntityTypeList
(clause 5.2.24) is to be returned, unless
details=true is specified.
EntityType[] 1 200 OK If details=true is specified, a response
body containing a JSON-LD array with
elements of the EntityType data structure
(clause 5.2.25) is to be returned.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Retrieve Available Entity Types](/api-operations/consumption/retrieve-available-entity-types.md)
* [Operation: Retrieve Details of Available Entity Types](/api-operations/consumption/retrieve-details-of-available-entity-types.md)
* [Clause 5.2.24](/api-operations/data-types/entitytypelist.md)
* [Clause 5.2.25](/api-operations/data-types/entitytype.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.25](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: types/
