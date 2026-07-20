---
type: NGSI-LD Operation
title: "Update Snapshot Status"
description: "5.16.4.1 Description This operation allows updating an existing Snapshot."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.16.4
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.16.4"
---

5.16.4.1 Description

This operation allows updating an existing Snapshot.

5.16.4.2 Use case diagram

A Context Consumer can update an existing Snapshot within an NGSI-LD system as shown in Figure 5.16.4.2-1.

Figure 5.16.4.2-1: Update Snapshot Status use case

5.16.4.3 Input data

- Snapshot identifier (URI), the target Snapshot. - A JSON-LD document representing a Snapshot Fragment

5.16.4.4 Behaviour

- If the Snapshot id is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.


- If the NGSI-LD System does not know about the target Snapshot, because there is no existing Snapshot whose id (URI) is equivalent, an error of type ResourceNotFound shall be raised.
- Execute the behaviour defined in clause 5.5.4 on JSON-LD validation.
- If the data types and restrictions expressed by clause 5.2.41 are not met by the Snapshot Fragment - in particular whether elements can be updated - then an error of type BadRequestData shall be raised.
- Term to URI expansion of Attribute names shall be observed as mandated by clause 5.5.7.
- Then, implementations shall modify the target Snapshot as mandated by clause 5.5.8.

5.16.4.5 Output data

A JSON-LD object representing the Snapshot status as mandated by clause 5.2.41.

# Related

* [HTTP: Resource: snapshots/{snapshotId}](/http-binding/resource-snapshots-snapshotid.md)
* [Clause 5.2.41](/api-operations/data-types/snapshot.md)
* [Clause 5.5.4](/api-operations/common-behaviours/general-ngsi-ld-validation.md)
* [Clause 5.5.7](/api-operations/common-behaviours/term-to-uri-expansion-or-compaction.md)
* [Clause 5.5.8](/api-operations/common-behaviours/partial-update-patch-behaviour.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.16.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Update Snapshot Status
