---
type: NGSI-LD Resource
title: "Resource: entityOperations/create"
description: "6.14.1 Description A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity creation for the NGSI-LD API."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.14
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.14"
---

6.14.1 Description

A sub-resource, pertaining to the entityOperations/ resource, intended to enable batch entity creation for the NGSI-LD API.


6.14.2 Resource definition

Resource URI: - /entityOperations/create

6.14.3 Resource methods

6.14.3.1 POST
This method is associated to the operation "Batch Entity Creation" and shall exhibit the behaviour defined by
clause 5.6.7. Figure 6.14.3.1-1 shows the operation interaction and Table 6.14.3.1-1 describes the request body and
possible responses.

Figure 6.14.3.1-1: Batch Entity Creation interaction

Table 6.14.3.1-1: Batch Entity Creation request body and possible responses

Request Body

Data Type Cardinality Remarks Entity[] 1 Array of entities to be created.

| | Data Type Cardinality | Response Code | Remarks | | |
| --- | --- | --- | --- | --- | --- |
| | String[] 1 | 201 Created | If all entities have been successfully | | |
| | | | created, an array of Strings containing URIs | | |

Data Type Cardinality Response Code Remarks
String[] 1 201 Created If all entities have been successfully
created, an array of Strings containing URIs
is returned in the response. Each URI
represents the Entity ID of a created entity.
There is no restriction as to the order of the
Entity IDs.
BatchOperationResult 1 207 Multi-Status If only some or none of the entities have
been successfully created, a response body
containing the result of each operation
contained in the batch is returned in a
BatchOperationResult structure. It contains
two arrays. The first array (success)
contains the URIs of the successfully
created entities, while the second array
(errors) contains information about the error
for each of the entities that could not be
created. There is no restriction as to the
order of the Entity IDs in the arrays.
If any of the entities matches to a
registration, the relevant parts of the request
are forwarded as a distributed operation.
In the case when an error response is
received back from any distributed
operation, a response body containing the
result returned from each registration is
returned in a BatchOperationResult
structure.
Errors can occur whenever a distributed
operation is unsupported, fails or times out,
see clause 6.3.17.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Batch Entity Creation](/api-operations/provision/batch-entity-creation.md)
* [Clause 5.6.7](/api-operations/provision/batch-entity-creation.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.14](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: entityOperations/create
