---
type: NGSI-LD Resource
title: "Resource: entityOperations/merge"
description: "6.31.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity merge for the NGSI-LD API."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.31
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.31"
---

6.31.1 Description

A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity merge for the NGSI-LD API.

6.31.2 Resource definition

Resource URI: - /entityOperations/merge

6.31.3 Resource methods

6.31.3.1 POST
This method is associated to the operation "Batch Entity Merge" and shall exhibit the behaviour defined by
clause 5.6.20. Figure 6.31.3.1-1 shows the operation interaction and Table 6.31.3.1-1 describes the request body and
possible responses.


Figure 6.31.3.1-1: Batch Entity Merge interaction

Table 6.31.3.1-1: Batch Entity Merge request body and possible responses

Request Body

Data Type Cardinality Remarks Entity[] 1 Array of Entities to be merged.

Response Body

Data Type Cardinality Response Code Remarks
N/A N/A 204 No Content If all entities have been successfully
merged, there is no payload body in the
response.
BatchOperationResult 1 207 Multi-Status If only some or none of the entities have
been successfully merged, a response body
containing the result of each operation
contained in the batch is returned in a
BatchOperationResult structure. It contains
two arrays. The first array (success)
contains the URIs of the successfully
merged entities, while the second array
(errors) contains information about the error
for each of the entities that could not be
merged-patched. There is no restriction as
to the order of the Entity IDs in the arrays.
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
operation is unsupported, fails or times out,
see clause 6.3.17.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

POST /entityOperations/merge

Entity[]

207 Multi-Status

BatchOperationResult

204 No Content

Possible non-error responses

# Related

* [Operation: Batch Entity Merge](/api-operations/provision/batch-entity-merge.md)
* [Clause 5.6.20](/api-operations/provision/batch-entity-merge.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.31](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entityOperations/merge
