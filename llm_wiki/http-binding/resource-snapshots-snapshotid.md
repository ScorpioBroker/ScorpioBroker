---
type: NGSI-LD Resource
title: "Resource: snapshots/{snapshotId}"
description: "6.37.1 Description This resource represents a snapshot in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.37
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.37"
---

6.37.1 Description

This resource represents a snapshot in an NGSI-LD system.

6.37.2 Resource definition

Resource URI:

- /snapshots/{snapshotId} Resource URI variables for this resource are defined in Table 6.37.2-1.

Table 6.37.2-1: URI variables Name Definition snapshotId Id (URI) of the Snapshot whose status is to be retrieved or updated, or the Snapshot to be deleted.

6.37.3 Resource methods

6.37.3.1 GET This method is associated to the operation "Retrieve Snapshot Status" and shall exhibit the behaviour defined by clause 5.16.3. The Snapshot identifier is the value of the resource URI variable "snapshotId". Figure 6.37.3.1-1 shows the Retrieve Snapshot Status interaction and Table 6.37.3.1-1 describes the request body and possible responses.

Figure 6.37.3.1-1: Retrieve Snapshot Status interaction

Table 6.37.3.1-1: Retrieve Snapshot Status request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks Snapshot 1 200 OK A response body containing the JSON-LD representation of the target Snapshot status.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an EntityMap identifier not known to the system, see clause 6.3.2.


6.37.3.2 PATCH This method is associated to the operation "Update Snapshot Status" and shall exhibit the behaviour defined by clause 5.16.4. The Snapshot identifier is the value of the resource URI variable "snapshotId". Figure 6.37.3.2-1 shows the Update Snapshot Status interaction and Table 6.37.3.2-1 describes the request body and possible responses.

Figure 6.37.3.2-1: Update Snapshot Status interaction

Table 6.37.3.2-1: Update Snapshot Status request body and possible responses

Request Body

Data Type Cardinality Remarks Snapshot Fragment 1 Payload body in the request contains a JSON-LD object which represents the Snapshot fragment with which the Snapshot status is to be updated.

Response Body

Data Type Cardinality Response Codes Remarks Snapshot 1 200 OK A response body containing the JSON-LD representation of the updated Snapshot status.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided an EntityMap identifier not known to the system, see clause 6.3.2.

6.37.3.3 DELETE

This method is associated to the operation "Delete Snapshot" and shall exhibit the behaviour defined by clause 5.16.5. The Snapshot identifier is the value of the resource URI variable "snapshotId". Figure 6.37.3.3-1 shows the Delete Snapshot interaction and Table 6.37.3.3-1 describes the request body and possible responses.

Figure 6.37.3.3-1: Delete Snapshot interaction


Table 6.37.3.3-1: Delete Snapshot request body and possible responses

Request Body

Data Type Cardinality Remarks N/A N/A

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 204 No Content ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a snapshot identifier not known to the system, see clause 6.3.2.

# Related

* [Operation: Retrieve Snapshot Status](/api-operations/snapshots/retrieve-snapshot-status.md)
* [Operation: Update Snapshot Status](/api-operations/snapshots/update-snapshot-status.md)
* [Operation: Delete Snapshot](/api-operations/snapshots/delete-snapshot.md)
* [Clause 5.16.3](/api-operations/snapshots/retrieve-snapshot-status.md)
* [Clause 5.16.4](/api-operations/snapshots/update-snapshot-status.md)
* [Clause 5.16.5](/api-operations/snapshots/delete-snapshot.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.37](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: snapshots/{snapshotId}
