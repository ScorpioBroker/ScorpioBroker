---
type: NGSI-LD Resource
title: "Resource: entityOperations/upsert"
description: "6.15.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity creation or update for the NGSI-LD API."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.15
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.15"
---

6.15.1 Description

A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity creation or update for the NGSI-LD API.

6.15.2 Resource definition

Resource URI:

- /entityOperations/upsert


6.15.3 Resource methods

6.15.3.1 POST
This method is associated to the operation "Batch Entity Creation or Update (Upsert)" and shall exhibit the behaviour
defined by clause 5.6.8. Figure 6.15.3.1-1 shows the operation interaction and Table 6.15.3.1-1 describes the request
body and possible responses.
The options query parameter for this request can take the following values:
- "replace". Indicates that all the existing Entity content shall be replaced (default mode);
- "update". Indicates that existing Entity content shall be updated.

Figure 6.15.3.1-1: Batch Entity Creation or Update interaction


Table 6.15.3.1-1: Batch Entity Creation or Update request body and possible responses

Request Body

Data Type Cardinality Remarks Entity[] 1 Array of entities to be created/updated.

Response Body

Data Type Cardinality Response Code Remarks
String[] 1 201 Created If all entities not existing prior to this
request have been successfully created
and the others have been successfully
updated, an array of String (with the URIs
representing the Entity IDs of the created
entities only) is returned in the response.
There is no restriction as to the order of
the Entity IDs. The merely updated entities
do not take part in the response
(corresponding to 204 No Content
returned in the case of updates).
N/A N/A 204 No Content If all entities already existed and are
successfully updated, there is no payload
body in the response.
BatchOperationResult 1 207 Multi-Status If only some or none of the entities have
been successfully created or updated, a
response body containing the result of
each operation contained in the batch is
returned in a BatchOperationResult
structure. It contains two arrays. The first
array (success) contains the URIs of the
successfully created or updated entities,
while the second array (errors) contains
information about the error for each of the
entities that could not be created or
updated. There is no restriction as to the
order of the Entity IDs in the arrays.
If any of the entities matches to a
registration, the relevant parts of the
request are forwarded as a distributed
operation.
In the case when an error response is
received back from any distributed
operation, a response body containing the
result returned from each registration is
returned in a BatchOperationResult
structure.
Errors can occur whenever a distributed
operation is unsupported, fails or times
out, see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Batch Entity Creation or Update (Upsert)](/api-operations/provision/batch-entity-creation-or-update-upsert.md)
* [Clause 5.6.8](/api-operations/provision/batch-entity-creation-or-update-upsert.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.15](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entityOperations/upsert
