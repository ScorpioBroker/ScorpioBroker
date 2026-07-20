---
type: NGSI-LD Operation
title: "Retrieve Snapshot Status"
description: "5.16.3.1 Description This operation allows retrieving the status of a Snapshot stored in an NGSI-LD system."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.16.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.16.3"
---

5.16.3.1 Description

This operation allows retrieving the status of a Snapshot stored in an NGSI-LD system.

5.16.3.2 Use case diagram

A Context Consumer can retrieve the status of a Snapshot from an NGSI-LD system as shown in Figure 5.16.3.2-1.

Figure 5.16.3.2-1: Retrieve Snapshot Status use case


5.16.3.3 Input data

Snapshot Id (URI) of the Snapshot whose status is to be retrieved.

5.16.3.4 Behaviour

- If the Snapshot Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the identifier provided does not correspond to any existing Snapshot in the system, then an error of type ResourceNotFound shall be raised.
- Otherwise, implementations shall retrieve the Snapshot status and return it to the caller.

5.16.3.5 Output data

A JSON-LD object representing the Snapshot status as mandated by clause 5.2.41.

# Related

* [HTTP: Resource: snapshots/{snapshotId}](/http-binding/resource-snapshots-snapshotid.md)
* [Clause 5.2.41](/api-operations/data-types/snapshot.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.16.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve Snapshot Status
