---
type: NGSI-LD Clause
title: "Detailed Attribute List Representation"
description: "The detailed attribute list representation is used to consume detailed information about attributes including the names of entity types that have instances with attributes, which have the respective a"
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.14
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.14"
---

The detailed attribute list representation is used to consume detailed information about attributes including the names of
entity types that have instances with attributes, which have the respective attribute name. The detailed attribute list
representation shall be an array of JSON-LD objects containing the following members:
Mandatory
- "attributeName": the URI that identifies the attribute (short name in case of availability in @context).
- "id": whose value shall be the URI that identifies the attribute.
- "type": the fixed value "Attribute". Optional
- "@context": a JSON-LD @context as described in clause 4.4.
- "typeNames": an array of the names of entity types that have instances with attributes, which have the respective attribute name.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.14](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Detailed Attribute List Representation
