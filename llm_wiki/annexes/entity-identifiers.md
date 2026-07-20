---
type: Reference
title: "Entity identifiers"
description: "In order to enable the participation of NGSI-LD in linked data scenarios, all Entities are identified by URIs."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-A.2
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "A.2"
---

In order to enable the participation of NGSI-LD in linked data scenarios, all Entities are identified by URIs. If those URIs are expected to participate in external linked data relationships they should be dereferenceable. It is noteworthy that the identifier from the point of view of NGSI-LD is different from the inherent identifier that a specific Entity may have. For instance, an NGSI-LD Entity of Type "Vehicle" may have a Property named licencePlateNumber, which it is actually a unique identifier from the point of view of the Entity domain, as it uniquely identifies the specific vehicle instance. However, from the point of view of the NGSI-LD system, it may have another identifier which might or might not include such licence plate number identifier.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause A.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity identifiers
