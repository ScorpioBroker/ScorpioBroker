---
type: NGSI-LD Resource
title: "Resource: snapshots"
description: "6.36.1 Description This resource represents Snapshots available in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.36
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.36"
---

6.36.1 Description

This resource represents Snapshots available in an NGSI-LD system.

6.36.2 Resource definition

Resource URI:

- /snapshots/

6.36.3 Resource methods

6.36.3.1 POST
This method is associated to the operation "Create Snapshot" and shall exhibit the behaviour defined by clause 5.16.1.
Figure 6.36.3.1-1 shows the operation interaction and Table 6.36.3.1-1 describes the request body and possible
responses.

Figure 6.36.3.1-1: Create Snapshot interaction

Table 6.36.3.1-1: Create Snapshot request body and possible responses

Request Body

Data Type Cardinality Remarks Snapshot 1 Payload body in the request contains a JSON-LD object which represents parameters for the snapshot to be created.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the relative path of the created snapshot.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 409 Conflict It is used to indicate that a Snapshot having the Snapshot identifier included in the request body already exists, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

6.36.3.2 DELETE

This method is associated to the operation "Purge Snapshots" and shall exhibit the behaviour defined by clause 5.16.7.
Figure 6.36.3.2-1 shows the Purge Snapshot interaction.

Figure 6.36.3.2-1: Purge Snapshots interaction

The URL parameters that shall be supported by implementations are those defined in Table 6.36.3.2-1 and Table 6.36.3.2-2 describes the request body and possible responses.

Table 6.36.3.2-1: Purge Snapshots URL parameters Name Data Type Cardinality Remarks q String 1 Query as per clause 4.9, restricted to members of the Snapshot data type.

Table 6.36.3.2-2: Purge Snapshots request body and possible responses

| | | | Data Type | | Cardinality | | | Remarks | | | | |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Request Body | | | | | | | | | | | | |
| | | | N/A | | N/A | | | | | | | |
| | | | Data Type | | Cardinality | | Response Codes | Remarks | | | | |
| | | | N/A | | N/A | | 204 No Content | | | | | |
| | | | BatchOperationResult | | 1 | | 207 Multi-Status | If some or all of the Snapshots have not | | | | |

Data Type Cardinality Response Codes Remarks
N/A N/A 204 No Content
BatchOperationResult 1 207 Multi-Status If some or all of the Snapshots have not
been successfully deleted, or did not
exist, a response body containing the
result of each operation contained in the
batch is returned in a
BatchOperationResult structure. It
contains two arrays. The first array
(success) contains the URIs of the
successfully deleted Snapshots, while
the second array (errors) contains
information about the error for each of
the Snapshots that could not be deleted.
There is no restriction as to the order of
the Snapshots IDs in the arrays.

Response Body

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a filter not matching any Snapshots, see clause 6.3.2.

# Related

* [Operation: Create Snapshot](/api-operations/snapshots/create-snapshot.md)
* [Operation: Purge Snapshots](/api-operations/snapshots/purge-snapshots.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
* [Clause 5.16.1](/api-operations/snapshots/create-snapshot.md)
* [Clause 5.16.7](/api-operations/snapshots/purge-snapshots.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.36](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: snapshots
