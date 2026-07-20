---
type: NGSI-LD Operation
title: "Delete EntityMap"
description: "5.14.3.1 Description This operation allows deleting an NGSI-LD EntityMap."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-5.14.3
tags: [ngsi-ld, cim009, v1.9.1, api]
timestamp: 2026-07-18T00:00:00Z
clause: "5.14.3"
---

5.14.3.1 Description

This operation allows deleting an NGSI-LD EntityMap.

5.14.3.2 Use case diagram

A client can request the broker to completely delete an EntityMap held within the NGSI-LD system as shown in Figure 5.14.3.2-1.

Figure 5.14.3.2-1: Delete EntityMap

5.14.3.3 Input data

EntityMap ID (URI) of the EntityMap to be retrieved (target EntityMap).

5.14.3.4 Behaviour

- If the EntityMap ID is not present or it is not a valid URI, then an error of type BadRequestData shall be raised.
- If the NGSI-LD endpoint does not know about a matching EntityMap for the EntityMap ID, then an error of type ResourceNotFound shall be raised.
- The EntityMap shall be removed from the broker's internal storage, or memory.

5.14.3.5 Output data

None.

Client

Delete EntityMap

NGSI-LD Client

NGSI-LD System deleted

1..*

delete entity map (entityMapId)

# Related

* [HTTP: Resource: entityMaps/{entityMapId}](/http-binding/resource-entitymaps-entitymapid.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 5.14.3](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Delete EntityMap
