---
type: NGSI-LD Resource
title: "Resource: temporal/entities/{entityId}/attrs/"
description: "6.20.1 Description This resource represents all the Attributes (Properties or Relationships) of a Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.20
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.20"
---

6.20.1 Description

This resource represents all the Attributes (Properties or Relationships) of a Temporal Evolution of an Entity.

6.20.2 Resource definition

Resource URI:

- /temporal/entities/{entityId}/attrs/ Resource URI variables for this resource are defined in Table 6.20.2-1.

Table 6.20.2-1: URI variables Name Definition entityId Id (URI) of the concerned entity

6.20.3 Resource methods

6.20.3.1 POST
This method is bound to the "Add Attributes to Temporal Evolution of an Entity" operation and shall exhibit the
behaviour defined by clause 5.6.12. The Entity identifier is the value of the resource URI variable entityId. The data to
be added shall be contained in the HTTP request payload body. Figure 6.20.3.1-1 shows the Add Attributes interaction
and Table 6.20.3.1-1 describes the request body and possible responses.


Figure 6.20.3.1-1: Add Attributes to Temporal Evolution of an Entity interaction

Table 6.20.3.1-1: Add Attributes to Temporal Evolution of an Entity request body and possible responses

Request Body

Data Type Cardinality Remarks EntityTemporal Fragment

1 EntityTemporal Fragment containing a complete representation of the Attribute instances to be added.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No content All the Attributes were added successfully.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) not known to the system, see clause 6.3.2.

# Related

* [Operation: Add Attributes to Temporal Evolution of an Entity](/api-operations/provision/add-attributes-to-temporal-evolution-of-an-entity.md)
* [Clause 5.6.12](/api-operations/provision/add-attributes-to-temporal-evolution-of-an-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.20](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entities/{entityId}/attrs/
