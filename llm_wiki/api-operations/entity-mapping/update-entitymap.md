---
type: NGSI-LD Operation
title: "Update EntityMap"
description: "5.14.2.1 Description This operation allows performing a partial update on an NGSI-LD EntityMap."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.14.2
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.14.2"
---

5.14.2.1 Description

This operation allows performing a partial update on an NGSI-LD EntityMap. A partial update only changes the elements provided in the EntityMap Fragment, leaving the rest as they are.

5.14.2.2 Use case diagram

A client can request the broker to update an EntityMap which is currently stored in the broker's internal storage as shown in Figure 5.14.2.2-1.

Figure 5.14.2.2-1: Update EntityMap

5.14.2.3 Input data

- EntityMap ID (URI) of the EntityMap to be retrieved (target EntityMap).
- A JSON-LD document representing an EntityMap.

5.14.2.4 Behaviour

- If the EntityMap ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about a matching EntityMap for the EntityMap ID, then an error of type ResourceNotFound shall be raised.


- Perform an update operation on the target EntityMap using the fields specified within then JSON-LD document. Any provided output-only fields shall be ignored.

5.14.2.5 Output data

None.

# Related

* [HTTP: Resource: entityMaps/{entityMapId}](/http-binding/resource-entitymaps-entitymapid.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.14.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Update EntityMap
