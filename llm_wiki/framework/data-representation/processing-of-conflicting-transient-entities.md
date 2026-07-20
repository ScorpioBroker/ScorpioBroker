---
type: NGSI-LD Clause
title: "Processing of Conflicting Transient Entities"
description: "In case of conflicting information when an Entity is received from a registered Context Source and marked with an expiresAt DateTime, but this expiresAt is not duplicated across all versions of the En"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.5.2
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.5.2"
---

In case of conflicting information when an Entity is received from a registered Context Source and marked with an expiresAt DateTime, but this expiresAt is not duplicated across all versions of the Entity received across all registered Context Sources, the following mechanism shall be used to differentiate. For each version of the Entity received where the expiresAt non-reified attribute is present at the Entity level:

- If no expiresAt attribute is present as a property of an Attribute, a non-reified expiresAt attribute is added as a property of that Attribute corresponding to the expiresAt found on the Entity.
- If the expiresAt attribute is present as a property of an Attribute, and the value on the Attribute is further in the future than the expiresAt value found on the Entity, the value of the expiresAt on the Attribute is overwritten to corresponding to the earlier DateTime.

# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.5.2](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Processing of Conflicting Transient Entities
