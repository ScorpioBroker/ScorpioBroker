---
type: NGSI-LD Resource
title: "Resource: snapshots/{snapshotId}/clone"
description: "6.38.1 Description This resource enables the cloning of a snapshot in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-6.38
tags: [ngsi-ld, cim009, v1.9.1, http]
timestamp: 2026-07-18T00:00:00Z
clause: "6.38"
---

6.38.1 Description

This resource enables the cloning of a snapshot in an NGSI-LD system.

6.38.2 Resource definition

Resource URI:

- /snapshots/{snapshotId}/clone Resource URI variables for this resource are defined in Table 6.38.2-1.

Table 6.38.2-1: URI variables Name Definition snapshotId Id (URI) of the Snapshot to be queried or deleted.

6.38.3 Resource methods

6.38.3.1 POST This method is associated to the operation "Clone Snapshot" and shall exhibit the behaviour defined by clause 5.16.2. The Snapshot identifier is the value of the resource URI variable "snapshotId". Figure 6.38.3.1-1 shows the Clone Snapshot interaction and Table 6.38.3.1-1 describes the request body and possible responses.

Figure 6.38.3.1-1: Clone Snapshot interaction


Table 6.38.3.1-1: Clone Snapshot request body and possible responses

Request Body

Data Type Cardinality Remarks Snapshot 1 Payload body in the request contains a JSON-LD object which represents parameters for the cloned snapshot.

Response Body

Data Type Cardinality Response Codes Remarks N/A N/A 201 Created The HTTP response shall include a "Location" HTTP header that contains the relative path of the cloned snapshot.

ProblemDetails (see IETF RFC 7807 [10])

1 400 Bad Request It is used to indicate that the request or its content is incorrect, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

ProblemDetails (see IETF RFC 7807 [10])

1 404 Not Found It is used when a client provided a Snapshot identifier (URI) not known to the system, see clause 6.3.2.

ProblemDetails (see IETF RFC 7807 [10])

1 409 Conflict It is used to indicate that the Snapshot having the Snapshot identifier included in the request body already exists, see clause 6.3.2. In the returned ProblemDetails structure, the detail attribute should convey more information about the error.

# Related

* [Operation: Clone Snapshot](/api-operations/snapshots/clone-snapshot.md)
* [Clause 5.16.2](/api-operations/snapshots/clone-snapshot.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 6.38](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Resource: snapshots/{snapshotId}/clone
