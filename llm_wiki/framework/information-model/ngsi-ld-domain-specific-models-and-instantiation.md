---
type: NGSI-LD Clause
title: "NGSI-LD domain-specific models and instantiation"
description: "This clause is informative and is intended to illustrate the relationship between the NGSI-LD Information Model and NGSI-LD Domain-specific models."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.2.4
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.2.4"
---

This clause is informative and is intended to illustrate the relationship between the NGSI-LD Information Model and

NGSI-LD Domain-specific models.

Figure 4.2.4-1 shows an example of an NGSI-LD domain-specific model. Domain-specific models introduce the specific entity types required for a particular domain. Figure 4.2.4-1 shows the types "Car", "Parking", "Street", "Gate". Entity types can have further subtypes, e.g. "OffStreetParking" as subtype of "Parking".

Figure 4.2.4-1: Cross-Domain Ontology and instantiation

In addition, two different NGSI-LD Properties are introduced (hasState, reliability).

The adjacentTo Relationship links entities of type "Parking" with entities of type "Street".

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.2.4](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — NGSI-LD domain-specific models and instantiation
