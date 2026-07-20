---
type: NGSI-LD Operation
title: "Snapshot status notification behaviour"
description: "A Snapshot status notification allows the subscriber, typically the creator of the Snapshot, to be notified when the Snapshot is ready, or in case of any problems or updates."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.16.6
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.16.6"
---

A Snapshot status notification allows the subscriber, typically the creator of the Snapshot, to be notified when the Snapshot is ready, or in case of any problems or updates. Implementations shall exhibit the following behaviour:

- SnapshotNotification (clause 5.3.4) messages can only be sent, if the endpoint member is set.
- SnapshotNotification messages are sent to the URI specified in the endpoint member of the Snapshot status.
- The information in the receiverInfo member of the Snapshot status shall be added to the SnapshotNotification message in the way required for the binding protocol.
- Snapshot Status Notifications shall be sent after all the specified Snapshot queries have been executed, the query results have been integrated and the Snapshot status has been updated accordingly.
- Snapshot status Notifications shall also be sent after any Snapshot status update, e.g. informing about the actual updated expiresAt timestamp.
- The SnapshotNotification shall be as mandated by clause 5.3.4.

# Related

* [Clause 5.3.4](/api-operations/notifications/snapshotnotification.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.16.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Snapshot status notification behaviour
