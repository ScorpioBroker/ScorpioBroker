---
type: NGSI-LD Resource
title: "Resource: entities/{entityId}/attrs/{attrId}"
description: "6.7.1 Description This resource represents an attribute (Property or Relationship) of an NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.7
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.7"
---

6.7.1 Description

This resource represents an attribute (Property or Relationship) of an NGSI-LD Entity.

6.7.2 Resource definition

Resource URI:

- /entities/{entityId}/attrs/{attrId} Resource URI variables for this resource are defined in Table 6.7.2-1.


Table 6.7.2-1: URI variables Name Definition entityId Id (URI) of the concerned entity attrId Attribute name (Property or Relationship)

6.7.3 Resource methods

6.7.3.1 PATCH

This method is bound to the "Partial Attribute Update" operation and shall exhibit the behaviour defined by clause 5.6.4. The Entity identifier is the value of the resource URI variable "entityId". The attribute name is the value of the resource URI variable "attrId". The Entity Fragment shall be contained in the HTTP request payload body. Figure 6.7.3.1-1 shows the Partial Attribute Update interaction.

Figure 6.7.3.1-1: Partial Attribute Update interaction

The URL parameters that shall be supported are those defined in Table 6.7.3.1-1 and Table 6.7.3.2-2 describes the request body and possible responses.

Table 6.7.3.1-1: Partial Attribute Update URL parameters Name Data Type Cardinality Remarks type String 0..1 Selection of Entity Types as per clause 4.17.

NGSI-LD Client NGSI-LD System

PATCH /entities/{entityId}/attrs/{attrId} Entity Fragment

204 No Content

207 Multi-Status

UpdateResult

Possible non-error responses

Table 6.7.3.1-2: Partial Attribute Update request body and possible responses

Request Body

Data Type Cardinality Remarks Entity Fragment 1 Entity Fragment containing the elements of the attribute to be updated.

Remarks

Data Type Cardinality Response Codes

| | N/A | N/A | 204 No Content | The attribute was updated successfully. | | |
| --- | --- | --- | --- | --- | --- | --- |
| | UpdateResult | 1 | 207 | If the entity input data matches to a | | |
| | | | Multi-Status | registration, the relevant parts of the request | | |

If the entity input data matches to a registration, the relevant parts of the request are forwarded as a distributed operation. In the case when an error response is received back from any distributed operation, a response body containing the result returned from each registration is returned in a UpdateResult structure.

Response Body

Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request

It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier or attribute name not known to the system, see clause 6.3.2.

6.7.3.2 DELETE

This method is associated to the operation "Delete Attribute" and shall exhibit the behaviour defined by clause 5.6.5. The Entity identifier is the value of the resource URI variable "entityId". The attribute name is the value of the resource URI variable "attrId". Figure 6.7.3.2-1 shows the Delete Attribute interaction, Table 6.7.3.2-1 shows the delete parameters to be supported and Table 6.7.3.2-2 describes the request body and possible responses.

NGSI-LD Client NGSI-LD System

DELETE /entities/{entityId}/attrs/{attrId} Entity Fragment

204 No Content

207 Multi-Status

Possible non-error responses

UpdateResult

Figure 6.7.3.2-1: Delete Attribute interaction


Table 6.7.3.2-1: Delete Attribute URL parameters Name Data Type Cardinality Remarks datasetId String 0..1 Shall be a valid URI. Specifies the datasetId of the dataset to be deleted. deleteAll Boolean 0..1 If true, all attribute instances are deleted. Otherwise (default) only the Attribute instance specified by the datasetId is deleted. In case neither the deleteAll flag nor a datasetId is present, the default Attribute instance is deleted. type String 0..1 Selection of Entity Types as per clause 4.17.

Table 6.7.3.2-2: Delete Attribute request body and possible responses

| | | Data Type | | | Cardinality | | | Remarks | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | | | |
| | N/A | | | | N/A | | | | | | |
| | | Data Type | | | Cardinality | | Response Codes | Remarks | | | |
| | N/A | | | | N/A | | 204 No Content | | | | |
| | UpdateResult | | | | 1 | | 207 Multi-Status | If the entity input data matches to a | | | |
| | | | | | | | | registration, the relevant parts of the | | | |
| | | | | | | | | request are forwarded as a distributed | | | |

Data Type Cardinality Response Codes Remarks
N/A N/A 204 No Content
UpdateResult 1 207 Multi-Status If the entity input data matches to a
registration, the relevant parts of the
request are forwarded as a distributed
operation.
In the case when an error response is
received back from any distributed
operation, a response body
containing the result returned from
each registration is returned in a
UpdateResult structure.

Response Body

Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) or attribute name not known to the system. see clause 6.3.2.

6.7.3.3 PUT
This method is bound to the "Replace Attribute" operation and shall exhibit the behaviour defined by clause 5.6.19. The
Entity identifier is the value of the resource URI variable "entityId". The attribute name is the value of the resource URI
variable "attrId". The Attribute Fragment shall be contained in the HTTP request payload body. Figure 6.7.3.3-1 shows
the Replace Attribute interaction.


PUT /entities/{entityId}/attrs/{attrId} Entity Fragment

204 No Content

207 Multi-Status

Possible non-error responses

UpdateResult

Figure 6.7.3.3-1: Replace Attribute interaction

The URL parameters that shall be supported are those defined in Table 6.7.3.3-1 and Table 6.7.3.3-2 describes the request body and possible responses.

Table 6.7.3.3-1: Replace Attribute URL parameters Name Data Type Cardinality Remarks type String 0..1 Selection of Entity Types as per clause 4.17.

Table 6.7.3.3-2: Replace Attribute request body and possible responses

| | Data Type | Cardinality | | | Remarks | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | |
| | Attribute Fragment | 1 | | Attribute Fragment replacing the previous data. | | | | |
| | Data Type | Cardinality | | Response Codes | | Remarks | | |
| | N/A | N/A | | 204 No Content | The attribute was replaced | | | |

Data Type Cardinality Response Codes Remarks
N/A N/A 204 No Content The attribute was replaced
successfully.
UpdateResult 1 207 Multi-Status If the entity input data matches to a
registration, the relevant parts of the
request are forwarded as a distributed
operation.
In the case when an error response is
received back from any distributed
operation, a response body
containing the result returned from
each registration is returned in a
UpdateResult structure.

Response Body

Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier or attribute name not known to the system, see clause 6.3.2.

# Related

* [Operation: Partial Attribute update](/api-operations/provision/partial-attribute-update.md)
* [Operation: Delete Attribute](/api-operations/provision/delete-attribute.md)
* [Operation: Replace Attribute](/api-operations/provision/replace-attribute.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 5.6.19](/api-operations/provision/replace-attribute.md)
* [Clause 5.6.4](/api-operations/provision/partial-attribute-update.md)
* [Clause 5.6.5](/api-operations/provision/delete-attribute.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entities/{entityId}/attrs/{attrId}
