---
type: NGSI-LD Operation
title: "Retrieve EntityMap"
description: "5.14.1.1 Description With this operation a client can obtain a cached EntityMap which is currently stored in the broker's internal storage, or memory."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.14.1
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.14.1"
---

5.14.1.1 Description

With this operation a client can obtain a cached EntityMap which is currently stored in the broker's internal storage, or memory.

5.14.1.2 Use case diagram

A client can request the broker to retrieve a specific EntityMap within the NGSI-LD system as shown in Figure 5.14.1.2-1.

Figure 5.14.1.2-1: Retrieve EntityMap

5.14.1.3 Input data

EntityMap ID (URI) of the EntityMap to be retrieved (target EntityMap).


5.14.1.4 Behaviour

- If the Entity ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about a matching EntityMap for the EntityMap ID, then an error of type ResourceNotFound shall be raised.
- Otherwise, return a JSON-LD object representing the EntityMap as mandated by clause 5.2.39.

5.14.1.5 Output data

A JSON-LD object representing the target EntityMap as mandated by clause 5.2.39.

# Related

* [HTTP: Resource: entityMaps/{entityMapId}](/http-binding/resource-entitymaps-entitymapid.md)
* [Clause 5.2.39](/api-operations/data-types/entitymap.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.14.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Retrieve EntityMap
