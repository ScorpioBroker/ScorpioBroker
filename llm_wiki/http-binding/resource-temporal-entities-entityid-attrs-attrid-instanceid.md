---
type: NGSI-LD Resource
title: "Resource: temporal/entities/{entityId}/attrs/{attrId}/ {instanceId}"
description: "6.22.1 Description This resource represents an Attribute (Property or Relationship) instance of a Temporal Evolution of an Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.22
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.22"
---

6.22.1 Description

This resource represents an Attribute (Property or Relationship) instance of a Temporal Evolution of an Entity.


6.22.2 Resource definition

Resource URI:

- /temporal/entities/{entityId}/attrs/{attrId}/{instanceId} Resource URI variables for this resource are defined in Table 6.22.2-1.

Table 6.22.2-1: URI variables Name Definition entityId Id (URI) of the concerned entity attrId Attribute name (Property or Relationship) instanceId Id (URI) identifying a particular Attribute instance

6.22.3 Resource methods

6.22.3.1 PATCH
This method is associated to the operation "Modify attribute instance from Temporal Evolution of an Entity" and shall
exhibit the behaviour defined by clause 5.6.14. The Entity identifier is the value of the resource URI variable entityId.
The attribute name is the value of the resource URI variable attrId. The instance identifier is the value of the resource
URI variable instanceId. Figure 6.22.3.1-1 shows the Modify Attribute instance interaction and Table 6.22.3.1-1
describes the request body and possible responses.

Figure 6.22.3.1-1: Modify Attribute instance from Temporal Evolution interaction

Table 6.22.3.1-1: Modify Attribute instance from Temporal Evolution request body and possible responses

| | | Data Type | | Cardinality | | Remarks | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | EntityTemporal | | 1 | EntityTemporal Fragment containing a complete | | | |
| | | Fragment | | | representation of the Attribute instance to be replaced. | | | |
| | | Data Type | | Cardinality | Response Codes | Remarks | | |
| | | N/A | | N/A | 204 No Content | | | |
| | | ProblemDetails (see | | 1 | 400 Bad Request | It is used to indicate that the request | | |
| | | IETF RFC 7807 [10]) | | | | or its content is incorrect, see | | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI), attribute name or instance identifier not known to the system. See clause 6.3.2.


6.22.3.2 DELETE

This method is associated to the operation "Delete Attribute instance from Temporal Evolution of an Entity" and shall exhibit the behaviour defined by clause 5.6.15. The Entity identifier is the value of the resource URI variable entityId. The Attribute name is the value of the resource URI variable attrId. The instance identifier is the value of the resource URI variable instanceId. Figure 6.22.3.2-1 shows the Delete Attribute instance interaction and Table 6.22.3.2-1 describes the request body and possible responses.

Figure 6.22.3.2-1: Delete Attribute instance from Temporal Evolution of an Entity interaction

Table 6.22.3.2-1: Delete Attribute instance from Temporal Evolution of an Entity request body and possible responses

| | | | Data Type | | Cardinality | | | Remarks | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | | |
| | | N/A | | | N/A | | | | | |
| | | | Data Type | | Cardinality | | Response Codes | Remarks | | |
| | | N/A | | | N/A | | 204 No Content | | | |
| | | ProblemDetails (see | | | 1 | | 400 Bad Request | It is used to indicate that the request | | |
| | | IETF RFC 7807 [10]) | | | | | | or its content is incorrect, see | | |

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI), attribute name or instance identifier not known to the system. See clause 6.3.2.

# Related

* [Operation: Modify Attribute instance in Temporal Evolution of an Entity](/api-operations/provision/modify-attribute-instance-in-temporal-evolution-of-an-entity.md)
* [Operation: Delete Attribute instance from Temporal Evolution of an Entity](/api-operations/provision/delete-attribute-instance-from-temporal-evolution-of-an-entity.md)
* [Clause 5.6.14](/api-operations/provision/modify-attribute-instance-in-temporal-evolution-of-an-entity.md)
* [Clause 5.6.15](/api-operations/provision/delete-attribute-instance-from-temporal-evolution-of-an-entity.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.22](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: temporal/entities/{entityId}/attrs/{attrId}/ {instanceId}
