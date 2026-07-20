---
type: NGSI-LD Resource
title: "Resource: attributes/{attrId}"
description: "6.28.1 Description This resource represents the specified attribute that belongs to entity instances existing within the NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.28
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.28"
---

6.28.1 Description

This resource represents the specified attribute that belongs to entity instances existing within the NGSI-LD system.

6.28.2 Resource definition

Resource URI:

- /attributes/{attrId} Resource URI variables for this resource are defined in Table 6.28.2-1.
Table 6.28.2-1: URI variables
Name Definition
attrId Name of the attribute for which detailed information is to be retrieved. The Fully Qualified Name (FQN) as well
as the short name can be used, given that the latter is part of the JSON-LD @context provided.

6.28.3 Resource methods

6.28.3.1 GET
This method is associated to the operation "Retrieve Available Attribute Information" and shall exhibit the behaviour
defined by clause 5.7.10. The attribute is the value of the resource URI variable "attrId". Figure 6.28.3.1-1 shows the
retrieve available attribute information interaction.

Figure 6.28.3.1-1: Retrieve Available Attribute Information interaction

Table 6.28.3.1-1 describes the request body and possible responses.

Table 6.28.3.1-1: Retrieve Available Attribute Information request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

| | Data Type | Cardinality | Response Codes | Remarks | |
| --- | --- | --- | --- | --- | --- |
| | Attribute | 1 | 200 OK | A response body containing the JSON-LD | |
| | | | | representation of the detailed information | |

about the available attribute.

ProblemDetails (see IETF RFC 7807 [10])

Response Body

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an attribute name not known to the system, see clause 6.3.2.

# Related

* [Operation: Retrieve Available Attribute Information](/api-operations/consumption/retrieve-available-attribute-information.md)
* [Clause 5.7.10](/api-operations/consumption/retrieve-available-attribute-information.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.28](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: attributes/{attrId}
