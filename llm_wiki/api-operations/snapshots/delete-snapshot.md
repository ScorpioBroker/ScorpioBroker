---
type: NGSI-LD Operation
title: "Delete Snapshot"
description: "5.16.5.1 Description This operation allows deleting an existing Snapshot."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.16.5
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.16.5"
---

5.16.5.1 Description

This operation allows deleting an existing Snapshot.

5.16.5.2 Use case diagram

A Context Consumer can delete a Snapshot within an NGSI-LD system as shown in Figure 5.16.5.2-1.

Figure 5.16.5.2-1: Delete Snapshot use case

5.16.5.3 Input data - A Snapshot identifier (URI), the target Snapshot.

5.16.5.4 Behaviour

- If the Snapshot Id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the Snapshot id provided does not correspond to any existing Snapshot in the system, then an error of type ResourceNotFound shall be raised.
- Otherwise implementations shall delete the Snapshot.


5.16.5.5 Output data

None.

# Related

* [HTTP: Resource: snapshots/{snapshotId}](/http-binding/resource-snapshots-snapshotid.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.16.5](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete Snapshot
