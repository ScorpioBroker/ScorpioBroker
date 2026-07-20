---
type: NGSI-LD Clause
title: "Introduction"
description: "Since Entities are uniquely identifiable by a URI, it is possible to traverse across the Entity graph directly from a Linking Entity to a Linked Entity."
resource: https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf#clause-4.5.23.1
tags: [ngsi-ld, cim009, v1.9.1, framework]
timestamp: 2026-07-18T00:00:00Z
clause: "4.5.23.1"
---

Since Entities are uniquely identifiable by a URI, it is possible to traverse across the Entity graph directly from a
Linking Entity to a Linked Entity. It is therefore sometimes convenient to be able to query or retrieve data
via a single Context Broker request and to receive a response including both Linking Entities and
dependent Linked Entities directly.
The concept of Entity graph retrieval is a common concept amongst graph databases and it allows for more structured
queries (see clause 4.9) and the complete serialization of an Entity and its dependents.
When retrieving Linked Entities, it is necessary to limit retrieval to avoid cascades of an excessive length,
duplicates or loops. Only Relationships targeting a locally stored Entity or Relationships annotated with an objectType
whose object is an Internal Linked Entity are considered to be retrievable in this manner.

# Related

* [Clause 4.9](/framework/languages/ngsi-ld-query-language.md)
# Citations

[1] [ETSI GS CIM 009 V1.9.1 clause 4.5.23.1](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_cim009v010901p.pdf) — Introduction
