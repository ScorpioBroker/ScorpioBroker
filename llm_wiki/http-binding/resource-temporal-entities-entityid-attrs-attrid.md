---
type: NGSI-LD Resource
title: "Resource: temporal/entities/{entityId}/attrs/{attrId}"
description: "6.21.1 Description This resource represents an Attribute (Property or Relationship) of a Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.21
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.21"
---

6.21.1 Description

This resource represents an Attribute (Property or Relationship) of a Temporal Evolution of an Entity.

6.21.2 Resource definition

Resource URI:

- /temporal/entities/{entityId}/attrs/{attrId} Resource URI variables for this resource are defined in Table 6.21.2-1.

Table 6.21.2-1: URI variables Name Definition entityId Id (URI) of the concerned entity attrId Attribute name (Property or Relationship)

6.21.3 Resource methods

6.21.3.1 DELETE

This method is associated to the operation "Delete Attribute from Temporal Evolution of an Entity" and shall exhibit the behaviour defined by clause 5.6.13. The Entity identifier is the value of the resource URI variable entityId. The Attribute name is the value of the resource URI variable attrId. Figure 6.21.3.1-1 shows the "Delete Attribute from Temporal Evolution of an Entity" interaction, Table 6.21.3.1-1 shows the delete parameters to be supported and Table 6.21.3.1-2 describes the request body and possible responses.

Figure 6.21.3.1-1: Delete Attribute from Temporal Evolution of an Entity interaction

Table 6.21.3.1-1: Delete Attribute from Temporal Evolution of an Entity URL parameters
name Data Type Cardinality Remarks
datasetId String 0..1 Shall be a valid URI. Specifies the datasetId of the dataset to be deleted.
deleteAll Boolean 0..1 If true, all attribute instances are deleted. Otherwise (default) only the Attribute
instance specified by the datasetId is deleted. In case neither the deleteAll flag
nor a datasetId is present, the default Attribute instance is deleted.

Table 6.21.3.1-2: Delete Attribute from Temporal Evolution of an Entity request body and possible responses

| | | Data Type | | | Cardinality | | | Remarks | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | |
| | N/A | | | | N/A | | | | |
| | | Data Type | | | Cardinality | | Response Codes | Remarks | |
| | N/A | | | | N/A | | 204 No Content | | |
| | ProblemDetails (see | | | | 1 | | 400 Bad Request | It is used to indicate that the request | |
| | IETF RFC 7807 [10]) | | | | | | | or its content is incorrect, see | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) or Attribute name not known to the system. See clause 6.3.2.

# Related

* [Operation: Delete Attribute from Temporal Evolution of an Entity](/api-operations/provision/delete-attribute-from-temporal-evolution-of-an-entity.md)
* [Clause 5.6.13](/api-operations/provision/delete-attribute-from-temporal-evolution-of-an-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.21](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entities/{entityId}/attrs/{attrId}
