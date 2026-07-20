---
type: NGSI-LD Clause
title: "Datatype Comparison Order"
description: "When sorting by value, and the values of Attributes are of mixed datatypes, the following comparison order (null values last), shall be implicitly applied: - Numbers - Strings - Object - Array - Boole"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.23.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.23.2"
---

When sorting by value, and the values of Attributes are of mixed datatypes, the following comparison order (null values last), shall be implicitly applied:

- Numbers
- Strings
- Object
- Array


- Boolean
- Time
- Date
- DateTime
- Null
- Attribute does not exist Additionally, when sorting by distance, the following comparison order, shall be applied:
- Attribute is a GeoProperty
- Attribute is not a GeoProperty - sorted by value as shown above.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.23.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Datatype Comparison Order
