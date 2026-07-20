---
type: NGSI-LD Resource
title: "Resource: types/{type}"
description: "6.26.1 Description This resource represents the specified entity type for which entity instances are available in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.26
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.26"
---

6.26.1 Description

This resource represents the specified entity type for which entity instances are available in an NGSI-LD system.

6.26.2 Resource definition

Resource URI:

- /types/{type} Resource URI variables for this resource are defined in Table 6.26.2-1.
Table 6.26.2-1: URI variables
Name Definition
type Name of the entity type for which detailed information is to be retrieved. The Fully Qualified Name (FQN) as
well as the short name can be used, given that the latter is part of the JSON-LD @context provided.

6.26.3 Resource methods

6.26.3.1 GET
This method is associated to the operation "Retrieve Available Entity Type Information" and shall exhibit the behaviour
defined by clause 5.7.7. The entity type is the value of the resource URI variable "type". Figure 6.26.3.1-1 shows the
retrieve available entity type interaction.

Figure 6.26.3.1-1: Retrieve Available Entity Type interaction

Table 6.26.3.1-1 describes the request body and possible responses.

Table 6.26.3.1-1: Retrieve Available Entity Type request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

| | Data Type | Cardinality | Response Codes | Remarks | | |
| --- | --- | --- | --- | --- | --- | --- |
| | EntityTypeInfo | 1 | 200 OK | A response body containing the JSON-LD | | |
| | | | | representation of the detailed information | | |

about the available entity type.

ProblemDetails (see IETF RFC 7807 [10])

Response Body

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an entity type not known to the system, see clause 6.3.2.

# Related

* [Operation: Retrieve Available Entity Type Information](/api-operations/consumption/retrieve-available-entity-type-information.md)
* [Clause 5.7.7](/api-operations/consumption/retrieve-available-entity-type-information.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.26](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: types/{type}
