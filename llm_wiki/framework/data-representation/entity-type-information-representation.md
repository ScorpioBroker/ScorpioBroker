---
type: NGSI-LD Clause
title: "Entity Type Information Representation"
description: "The entity type information representation is used to consume detailed information about an entity type."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.12
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.12"
---

The entity type information representation is used to consume detailed information about an entity type. The entity type information representation shall be a JSON-LD object containing the following members: Mandatory

- "id" whose value shall be the URI that identifies the entity type.
- "type": the fixed value "EntityTypeInfo".
- "typeName": the URI that identifies the entity type (short name in case of availability in @context). Optional
- "@context" a JSON-LD @context as described in clause 4.4.

# Related

* [Clause 4.4](/framework/core-and-user-ngsi-ld-at-context.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.12](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Entity Type Information Representation
