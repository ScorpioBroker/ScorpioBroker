---
type: Reference
title: "Maintain collations as metadata"
description: "- Create a subscription on the attribute (e.g."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-G.2.1
tags: [ngsi-ld, cim009, v1.9.1]
timestamp: 2026-07-18T00:00:00Z
clause: "G.2.1"
---

- Create a subscription on the attribute (e.g. name)
- Create a simple microservice to add/upsert a name.collate property-of-a-property using a simple function to strip all diacritic marks - for example: str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLower() Other substitutions could be made where local spelling rules vary (for example different for German ö = oe).

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause G.2.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Maintain collations as metadata
