---
type: NGSI-LD Clause
title: "Temporal Representation of an Entity"
description: "The temporal representation of an Entity is the way to represent the Temporal Evolution of an Entity: the Entity shall be as mandated by clause 4.5.1, but for each Property and Relationship their temp"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.6
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.6"
---

The temporal representation of an Entity is the way to represent the Temporal Evolution of an Entity: the Entity shall be as mandated by clause 4.5.1, but for each Property and Relationship their temporal representation shall be provided as mandated by clauses 4.5.7 and 4.5.8 and the Scope (if present) shall be represented as a temporal representation of a Property (clause 4.5.7) that can only have the non-reified Temporal Properties createdAt, modifiedAt, deletedAt and observedAt as sub-Properties. This is required to represent the Scope of a Temporal Evolution of an Entity. In case the Temporal Evolution of the Scope is updated as the result of a change from the Core API, the observedAt sub-Property should be set as a copy of the modifiedAt sub-Property.

# Related

* [Clause 4.5.1](/framework/data-representation/ngsi-ld-entity-representation.md)
* [Clause 4.5.7](/framework/data-representation/temporal-representation-of-a-property.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.6](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Temporal Representation of an Entity
