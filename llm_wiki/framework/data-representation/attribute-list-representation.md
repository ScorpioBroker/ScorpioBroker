---
type: NGSI-LD Clause
title: "Attribute List Representation"
description: "The attribute list representation is used to consume information about attributes."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.13
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.13"
---

The attribute list representation is used to consume information about attributes. The attribute list representation shall be
a JSON-LD object containing the following members:
Mandatory
- "attributeList": JSON-LD array containing the attribute names.
- "id": whose value shall be a URI that identifies the attribute list.
- "type": the fixed value "AttributeList". Optional
- "@context": a JSON-LD @context as described in clause 4.4.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.13](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Attribute List Representation
