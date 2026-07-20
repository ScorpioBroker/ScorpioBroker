---
type: NGSI-LD Resource
title: "Resource: entities/{entityId}/attrs/"
description: "6.6.1 Description This resource represents all the Attributes (Properties or Relationships) of an NGSI-LD Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.6
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.6"
---

6.6.1 Description

This resource represents all the Attributes (Properties or Relationships) of an NGSI-LD Entity.

6.6.2 Resource definition

Resource URI:

- /entities/{entityId}/attrs Resource URI variables for this resource are defined in Table 6.6.2-1.

Table 6.6.2-1: URI variables Name Definition entityId Id (URI) of the concerned entity


6.6.3 Resource methods

6.6.3.1 POST
This method is bound to the "Append Attributes" operation and shall exhibit the behaviour defined by clause 5.6.3. The
Entity identifier is the value of the resource URI variable "entityId". The data to be appended shall be contained in the
HTTP request payload body. Figure 6.6.3.1-1 shows the Append Attributes interaction.

Figure 6.6.3.1-1: Append Attributes interaction

The URL parameters that shall be supported are those defined in Table 6.6.3.1-1 and Table 6.6.3.1-2 describes the request body and possible responses.

Table 6.6.3.1-1: Append Attributes URL parameters Name Data Type Cardinality Remarks options Comma separated list of strings

0..1 "noOverwrite" indicates that no attribute overwrite shall be performed. type String 0..1 Selection of Entity Types as per clause 4.17.


Table 6.6.3.1-2: Append Attributes request body and possible responses

Request Body

Data Type Cardinality Remarks Entity Fragment 1 Entity Fragment containing a complete representation of the Attributes to be added.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No content All the Attributes were appended successfully. UpdateResult 1 207 Multi-Status Only the Attributes included in the response payload body were successfully appended.

If the entity input data matches to a registration, the relevant parts of the request are forwarded as a distributed operation. In the case when an error response is received back from any distributed operation, a response body containing the result returned from each registration is returned in a UpdateResult structure. Errors can occur whenever a distributed operation is unsupported, fails or times out, see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier (URI) not known to the system, see clause 6.3.2.

6.6.3.2 PATCH

This method is bound to the "Update Attributes" operation and shall exhibit the behaviour defined by clause 5.6.2. The Entity identifier is the value of the resource URI variable "entityId". The data to be updated shall be contained in the HTTP request payload body. Figure 6.6.3.2-1 shows the Update Attributes interaction.

Figure 6.6.3.2-1: Update Attributes interaction

The URL parameters that shall be supported are those defined in Table 6.6.3.2-1 and Table 6.6.3.2-2 describes the request body and possible responses.

Table 6.6.3.2-1: Update Attributes URL parameters Name Data Type Cardinality Remarks type String 0..1 Selection of Entity Types as per clause 4.17.

Table 6.6.3.2-2: Update Attributes request body and possible responses

| Request Body | | Entity Fragment | 1 | | Entity Fragment containing a complete representation of the | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | Attributes to be updated. | | | | |
| | | Data Type | Cardinality | | Response Codes | Remarks | | | |
| | | N/A | N/A | | 204 No content | All the Attributes were updated | | | |

Data Type Cardinality Remarks

Data Type Cardinality Response Codes Remarks
N/A N/A 204 No content All the Attributes were updated
successfully.
UpdateResult 1 207 Multi-Status Only the Attributes included in the
response payload body were
successfully updated. If no Attributes
were successfully updated the updated
array of UpdateResult (see
clause 5.2.18) will be empty.
If the entity input data matches to a
registration, the relevant parts of the
request are forwarded as a distributed
operation.
In the case when an error response is
received back from any distributed
operation, a response body containing
the result returned from each
registration is returned in a
BatchOperationResult structure.
Errors can occur whenever a distributed
operation is unsupported, fails or times
out, see clause 6.3.17.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an Entity identifier not known to the system, see clause 6.3.2.

# Related

* [Operation: Update Attributes](/api-operations/provision/update-attributes.md)
* [Operation: Append Attributes](/api-operations/provision/append-attributes.md)
* [Clause 4.17](/framework/languages/ngsi-ld-entity-type-selection-language.md)
* [Clause 5.2.18](/api-operations/data-types/updateresult.md)
* [Clause 5.6.2](/api-operations/provision/update-attributes.md)
* [Clause 5.6.3](/api-operations/provision/append-attributes.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entities/{entityId}/attrs/
