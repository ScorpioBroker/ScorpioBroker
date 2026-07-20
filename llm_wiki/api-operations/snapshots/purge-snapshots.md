---
type: NGSI-LD Operation
title: "Purge Snapshots"
description: "5.16.7.1 Description This operation allows purging selected Snapshots."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.16.7
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.16.7"
---

5.16.7.1 Description

This operation allows purging selected Snapshots.

5.16.7.2 Use case diagram

A Context Consumer can purge Snapshots within an NGSI-LD system as shown in Figure 5.16.7.2-1.

Figure 5.16.7.2-1: Purge Snapshots use case

5.16.7.3 Input data

- An NGSI-LD Query to select fitting Snapshots to be purged based on members of the Snapshot data type (see Table 5.2.41-1), as per clause 4.9.


5.16.7.4 Behaviour

- If the NGSI-LD Query is not present or it is not a valid as per clause 4.9, restricted to members of the Snapshot data type, then an error of type BadRequestData shall be raised.
- Implementations shall purge the Snapshots fitting the NGSI-LD Query.

5.16.7.5 Output data

None.

# Related

* [HTTP: Resource: snapshots](/http-binding/resource-snapshots.md)
* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.16.7](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Purge Snapshots
